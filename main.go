package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	mediapkg "github.com/livekit/media-sdk"
	lkproto "github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
	"github.com/pion/webrtc/v4"
)

const (
	sampleRate = 8000
	channels   = 1
)

type config struct {
	ListenAddr    string
	LiveKitURL    string
	APIKey        string
	APISecret     string
	AgentName     string
	SetupTimeout  time.Duration
	ShutdownGrace time.Duration
}

func loadConfig() (config, error) {
	c := config{
		ListenAddr:    env("LISTEN_ADDR", "127.0.0.1:8091"),
		LiveKitURL:    strings.TrimSpace(os.Getenv("LIVEKIT_URL")),
		APIKey:        strings.TrimSpace(os.Getenv("LIVEKIT_API_KEY")),
		APISecret:     strings.TrimSpace(os.Getenv("LIVEKIT_API_SECRET")),
		AgentName:     env("LIVEKIT_AGENT_NAME", "pentagonai"),
		SetupTimeout:  durationEnv("SETUP_TIMEOUT", 20*time.Second),
		ShutdownGrace: durationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	if c.LiveKitURL == "" || c.APIKey == "" || c.APISecret == "" {
		return c, errors.New("LIVEKIT_URL, LIVEKIT_API_KEY and LIVEKIT_API_SECRET are required")
	}
	return c, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return d
}

type mediaStart struct {
	Event            string            `json:"event"`
	ConnectionID     string            `json:"connection_id"`
	Channel          string            `json:"channel"`
	ChannelID        string            `json:"channel_id"`
	Format           string            `json:"format"`
	OptimalFrameSize int               `json:"optimal_frame_size"`
	Ptime            int               `json:"ptime"`
	ChannelVariables map[string]string `json:"channel_variables"`
}

type controlEvent struct {
	Event string `json:"event"`
	Digit string `json:"digit,omitempty"`
}

type callMetadata struct {
	HumanNumber string `json:"human_number"`
	AgentNumber string `json:"agent_number"`
	CallType    string `json:"call_type"`
	CallChannel string `json:"call_channel"`
	UniqueID    string `json:"asterisk_uniqueid"`
	LinkedID    string `json:"asterisk_linkedid"`
	Caller      string `json:"caller_number"`
	Callee      string `json:"callee_number"`
	TraceID     string `json:"trace_id"`
}

func metadataFrom(start mediaStart, query url.Values) (callMetadata, map[string]string) {
	get := func(key string) string {
		if value := strings.TrimSpace(start.ChannelVariables[key]); value != "" {
			return value
		}
		return strings.TrimSpace(query.Get(strings.ToLower(key)))
	}
	uniqueID := first(get("ASTERISK_UNIQUEID"), start.ChannelID)
	linkedID := first(get("ASTERISK_LINKEDID"), uniqueID)
	human := digits(first(get("CALL_HUMAN_NUMBER"), get("CALLER_NUMBER")))
	agent := digits(first(get("CALL_AGENT_NUMBER"), get("CALLEE_NUMBER")))
	caller := first(get("CALLER_NUMBER"), human)
	callee := first(get("CALLEE_NUMBER"), agent)
	traceID := first(get("CALL_TRACE_ID"), linkedID)
	meta := callMetadata{
		HumanNumber: human, AgentNumber: agent, CallType: "Inbound", CallChannel: "PBX",
		UniqueID: uniqueID, LinkedID: linkedID, Caller: caller, Callee: callee, TraceID: traceID,
	}
	attrs := map[string]string{
		"call.human_number": human, "call.agent_number": agent,
		"call.type": "Inbound", "call.channel": "PBX",
		"asterisk.uniqueid": uniqueID, "asterisk.linkedid": linkedID,
		"call.caller_number": caller, "call.callee_number": callee, "call.trace_id": traceID,
		// Temporary compatibility aliases. This remains an RTC participant.
		"sip.phoneNumber": human, "sip.trunkPhoneNumber": agent,
		"sip.callID": linkedID, "sip.callIDFull": uniqueID, "sip.callStatus": "active",
	}
	for key, value := range attrs {
		if value == "" {
			delete(attrs, key)
		}
	}
	return meta, attrs
}

func first(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func digits(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

type frameWriter struct {
	mu        sync.Mutex
	cond      *sync.Cond
	conn      *websocket.Conn
	frameSize int
	paused    bool
	answered  bool
	closed    bool
	buf       []byte
}

func newFrameWriter(conn *websocket.Conn, frameSize int) *frameWriter {
	w := &frameWriter{conn: conn, frameSize: frameSize}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *frameWriter) WriteSample(sample mediapkg.PCM16Sample) error {
	data := make([]byte, len(sample)*2)
	for i, value := range sample {
		binary.LittleEndian.PutUint16(data[i*2:], uint16(value))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.paused && !w.closed {
		w.cond.Wait()
	}
	if w.closed {
		return errors.New("asterisk websocket closed")
	}
	if !w.answered {
		if err := w.conn.WriteJSON(map[string]string{"command": "ANSWER"}); err != nil {
			return err
		}
		w.answered = true
	}
	w.buf = append(w.buf, data...)
	for len(w.buf) >= w.frameSize {
		if err := w.conn.WriteMessage(websocket.BinaryMessage, w.buf[:w.frameSize]); err != nil {
			return err
		}
		w.buf = w.buf[w.frameSize:]
	}
	return nil
}

func (w *frameWriter) setPaused(paused bool) {
	w.mu.Lock()
	w.paused = paused
	if !paused {
		w.cond.Broadcast()
	}
	w.mu.Unlock()
}

func (w *frameWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.buf = nil
	w.cond.Broadcast()
	w.mu.Unlock()
	return nil
}

func (w *frameWriter) hangup() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.conn.WriteJSON(map[string]string{"command": "HANGUP"})
}

type bridge struct {
	cfg      config
	log      *slog.Logger
	upgrader websocket.Upgrader
	active   sync.Map
}

func newBridge(cfg config, log *slog.Logger) *bridge {
	return &bridge{cfg: cfg, log: log, upgrader: websocket.Upgrader{
		Subprotocols: []string{"media"},
		CheckOrigin: func(r *http.Request) bool {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			return err == nil && net.ParseIP(host).IsLoopback()
		},
	}}
}

func (b *bridge) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (b *bridge) media(w http.ResponseWriter, r *http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		b.log.Warn("websocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()
	if err := b.serveCall(r.Context(), conn, r.URL.Query()); err != nil && !errors.Is(err, context.Canceled) {
		b.log.Error("call ended with error", "error", err)
	}
}

func (b *bridge) serveCall(parent context.Context, conn *websocket.Conn, query url.Values) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	_ = conn.SetReadDeadline(time.Now().Add(b.cfg.SetupTimeout))
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read MEDIA_START: %w", err)
	}
	if messageType != websocket.TextMessage {
		return errors.New("first websocket frame was not MEDIA_START")
	}
	var start mediaStart
	if err := json.Unmarshal(payload, &start); err != nil || start.Event != "MEDIA_START" {
		return fmt.Errorf("invalid MEDIA_START: %s", string(payload))
	}
	if start.Format != "slin" || start.OptimalFrameSize <= 0 || start.OptimalFrameSize%2 != 0 {
		return fmt.Errorf("unsupported media format=%q frame_size=%d", start.Format, start.OptimalFrameSize)
	}
	_ = conn.SetReadDeadline(time.Time{})
	meta, attrs := metadataFrom(start, query)
	if meta.LinkedID == "" || meta.HumanNumber == "" || meta.AgentNumber == "" {
		return errors.New("MEDIA_START missing linked ID or phone metadata")
	}
	if _, loaded := b.active.LoadOrStore(meta.LinkedID, struct{}{}); loaded {
		return fmt.Errorf("session already active for linked_id=%s", meta.LinkedID)
	}
	defer b.active.Delete(meta.LinkedID)
	log := b.log.With("linked_id", meta.LinkedID, "room", "call_"+meta.LinkedID, "participant", "pstn_"+meta.LinkedID)

	roomName := "call_" + safeID(meta.LinkedID)
	identity := "pstn_" + safeID(meta.LinkedID)
	metadataJSON, _ := json.Marshal(meta)
	writer := newFrameWriter(conn, start.OptimalFrameSize)
	mediaFrames := make(chan []byte, 1)
	controlFrames := make(chan []byte, 16)
	readErrors := make(chan error, 1)
	var droppedStartupFrames atomic.Uint64
	var liveMedia atomic.Bool
	go func() {
		for {
			typ, payload, readErr := conn.ReadMessage()
			if readErr != nil {
				readErrors <- readErr
				cancel()
				return
			}
			switch typ {
			case websocket.BinaryMessage:
				if liveMedia.Load() {
					select {
					case mediaFrames <- payload:
					case <-ctx.Done():
						return
					}
					continue
				}
				select {
				case mediaFrames <- payload:
				default:
					dropped := false
					select {
					case <-mediaFrames:
						dropped = true
					default:
					}
					select {
					case mediaFrames <- payload:
					default:
						dropped = true
					}
					if dropped {
						droppedStartupFrames.Add(1)
					}
				}
			case websocket.TextMessage:
				controlFrames <- payload
			}
		}
	}()

	api := lksdk.NewRoomServiceClient(b.cfg.LiveKitURL, b.cfg.APIKey, b.cfg.APISecret)
	_, err = api.CreateRoom(ctx, &lkproto.CreateRoomRequest{
		Name: roomName, EmptyTimeout: 60, DepartureTimeout: 20,
	})
	if err != nil {
		return fmt.Errorf("create room: %w", err)
	}

	callbacks := &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnTrackSubscribed: func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant) {
				if participant.Identity() == identity || track.Kind() != webrtc.RTPCodecTypeAudio {
					return
				}
				if _, err := lkmedia.NewPCMRemoteTrack(track, writer,
					lkmedia.WithTargetSampleRate(sampleRate), lkmedia.WithTargetChannels(channels)); err != nil {
					log.Error("remote audio subscription failed", "error", err)
				}
			},
		},
		OnDisconnected: cancel,
		OnParticipantDisconnected: func(participant *lksdk.RemoteParticipant) {
			if participant.Kind() == lksdk.ParticipantAgent {
				cancel()
			}
		},
	}
	room, err := lksdk.ConnectToRoom(b.cfg.LiveKitURL, lksdk.ConnectInfo{
		APIKey: b.cfg.APIKey, APISecret: b.cfg.APISecret, RoomName: roomName,
		ParticipantIdentity: identity, ParticipantName: meta.HumanNumber,
		ParticipantKind: lksdk.ParticipantStandard, ParticipantMetadata: string(metadataJSON),
		ParticipantAttributes: attrs,
	}, callbacks)
	if err != nil {
		return fmt.Errorf("join livekit room: %w", err)
	}
	defer room.Disconnect()

	localTrack, err := lkmedia.NewPCMLocalTrack(sampleRate, channels, logger.GetLogger())
	if err != nil {
		return fmt.Errorf("create PCM track: %w", err)
	}
	defer localTrack.Close()
	if _, err = room.LocalParticipant.PublishTrack(localTrack, &lksdk.TrackPublicationOptions{Name: "pstn_audio", Source: lkproto.TrackSource_MICROPHONE}); err != nil {
		return fmt.Errorf("publish PCM track: %w", err)
	}
	if err := b.ensureAgentDispatch(ctx, roomName, string(metadataJSON)); err != nil {
		return fmt.Errorf("dispatch agent: %w", err)
	}
	log.Info("call bridge ready", "frame_size", start.OptimalFrameSize, "ptime_ms", start.Ptime)

	liveMedia.Store(true)
	log.Info("startup audio drained", "dropped_frames", droppedStartupFrames.Load())
	var firstAudioAt, lastAudioStats time.Time
	var receivedSamples int64
	for {
		select {
		case <-ctx.Done():
			_ = writer.hangup()
			_ = writer.Close()
			return nil
		case <-readErrors:
			_ = writer.Close()
			return nil
		case payload := <-mediaFrames:
				if len(payload)%2 != 0 { return errors.New("odd-length PCM frame") }
				now := time.Now()
				if firstAudioAt.IsZero() {
					firstAudioAt, lastAudioStats = now, now
				}
				receivedSamples += int64(len(payload) / 2)
				if now.Sub(lastAudioStats) >= 5*time.Second {
					elapsed := now.Sub(firstAudioAt).Seconds()
					audioSeconds := float64(receivedSamples) / sampleRate
					log.Info("asterisk audio timing", "elapsed_s", elapsed, "audio_s", audioSeconds, "drift_s", audioSeconds-elapsed)
					lastAudioStats = now
				}
				samples := make(mediapkg.PCM16Sample, len(payload)/2)
				for i := range samples { samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:])) }
				if err := localTrack.WriteSample(samples); err != nil { return err }
		case payload := <-controlFrames:
			var event controlEvent
			if json.Unmarshal(payload, &event) != nil { continue }
			switch event.Event {
			case "MEDIA_XOFF": writer.setPaused(true)
			case "MEDIA_XON": writer.setPaused(false)
			case "DTMF_END": log.Info("dtmf received", "digit", event.Digit)
			}
		}
	}
}

func (b *bridge) ensureAgentDispatch(ctx context.Context, roomName, metadata string) error {
	client := lksdk.NewAgentDispatchServiceClient(
		b.cfg.LiveKitURL, b.cfg.APIKey, b.cfg.APISecret,
	)
	response, err := client.ListDispatch(ctx, &lkproto.ListAgentDispatchRequest{Room: roomName})
	if err != nil {
		return err
	}
	for _, dispatch := range response.AgentDispatches {
		if dispatch.AgentName == b.cfg.AgentName {
			return nil
		}
	}
	_, err = client.CreateDispatch(ctx, &lkproto.CreateAgentDispatchRequest{
		Room: roomName, AgentName: b.cfg.AgentName, Metadata: metadata,
	})
	return err
}

func safeID(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else { b.WriteByte('_') }
	}
	return b.String()
}

func main() {
	cfg, err := loadConfig()
	if err != nil { panic(err) }
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	b := newBridge(cfg, log)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", b.health)
	mux.HandleFunc("/media", b.media)
	server := &http.Server{Addr: cfg.ListenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Info("bridge listening", "addr", cfg.ListenAddr, "agent", cfg.AgentName)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { log.Error("server failed", "error", err); stop() }
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	_ = server.Shutdown(shutdown)
}
