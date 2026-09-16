# Implementation and production runbook

This service is a deliberately thin media bridge between Asterisk 20
`chan_websocket` and LiveKit. Asterisk remains the PBX and owns SIP, RTP,
codecs, transcoding, pacing, DTMF, routing, transfers, and call lifecycle. The
Go process moves signed-linear audio and call metadata between Asterisk and a
normal LiveKit RTC participant.

## Architecture and call flow

```text
PSTN/SIP trunk
    -> FreePBX and Asterisk 20
    -> chan_websocket (slin, PCM16, mono, 8 kHz)
    -> Go bridge on 127.0.0.1
    -> normal LiveKit RTC participant
    -> explicitly dispatched LiveKit agent
```

The production sequence is:

1. FreePBX routes a selected DID to a custom extension.
2. Asterisk stores `UNIQUEID`, `LINKEDID`, caller, callee, and trace values in
   inherited channel variables.
3. `chan_websocket` opens `/media` using the `media` subprotocol.
4. The bridge validates `MEDIA_START`, including format and
   `optimal_frame_size`.
5. It creates or joins `call_<linkedid>` as `pstn_<linkedid>`.
6. It publishes the caller PCM track, dispatches the configured agent, and
   subscribes to agent audio.
7. It sends `ANSWER` to Asterisk on the first agent PCM sample.
8. Disconnecting either side cancels all per-call work and releases resources.

The WebSocket listener should remain on loopback. It is not an Internet-facing
media service.

## Media handling

The Asterisk/Go boundary is `slin`: signed PCM16 little-endian, 8,000 Hz, mono.
The bridge uses Asterisk's advertised `optimal_frame_size` and buffers only
enough output to write complete frames. It does not create a pacing timer.

Caller audio uses `media.NewPCMLocalTrack(8000, 1, ...)`. Agent audio uses
`media.NewPCMRemoteTrack(...)` with an 8 kHz, mono target. The LiveKit SDK owns
Opus encoding/decoding and resampling.

Control behavior:

- `MEDIA_XOFF` pauses writes to Asterisk.
- `MEDIA_XON` resumes them.
- `DTMF_END` is consumed as a control event, never decoded from PCM.

## Startup latency

An early implementation waited for LiveKit setup before draining Asterisk
media. Roughly 2.7 seconds accumulated and was replayed late, causing permanent
conversation lag.

The current reader starts immediately after `MEDIA_START`. While LiveKit is
initializing, its one-frame startup queue keeps the newest media rather than a
stale backlog. Once ready, every frame is delivered in order. The Asterisk
dial string includes `n` to suppress automatic answer; the bridge issues
`ANSWER` only with the first agent PCM. The caller therefore hears ringing
during setup instead of answered silence.

## Participant and metadata contract

The bridge joins with `ParticipantStandard`, not `ParticipantSIP`.

Canonical attributes:

- `call.human_number`
- `call.agent_number`
- `call.type=Inbound`
- `call.channel=PBX`
- `call.caller_number`
- `call.callee_number`
- `call.trace_id`
- `asterisk.uniqueid`
- `asterisk.linkedid`

Flat JSON participant metadata carries the same values. Temporary aliases
(`sip.phoneNumber`, `sip.trunkPhoneNumber`, `sip.callID`, `sip.callIDFull`, and
`sip.callStatus`) support older agents but do not change participant kind.

Room names and participant identities are derived from sanitized `LINKEDID`.
The bridge rejects a duplicate active local `LINKEDID` and checks existing room
dispatches before creating the configured agent dispatch.

## Dependencies

| Dependency | Version | Role |
|---|---:|---|
| Go | 1.24.2 module target | Runtime and concurrency |
| Asterisk | 20.21.0 production version | PBX and `chan_websocket` |
| `gorilla/websocket` | 1.5.3 | Asterisk WebSocket endpoint |
| `livekit/server-sdk-go/v2` | 2.9.2 | Rooms, media helpers, dispatch |
| `livekit/media-sdk` | pinned pseudo-version | PCM samples |
| `livekit/protocol` | pinned 1.39.4 prerelease | LiveKit protocol types |
| `pion/webrtc/v4` | 4.1.3 | Remote audio-track types |
| systemd | host version | Process isolation and supervision |

The standard library provides HTTP, JSON, binary PCM conversion, cancellation,
synchronization, signals, and structured logging.

## FreePBX-safe deployment

Use only custom configuration and FreePBX-managed routes. Never edit generated
files such as `extensions_additional.conf`.

The sample files in `deploy/` define:

- a per-call WebSocket client targeting `ws://127.0.0.1:8091/media`
- JSON `chan_websocket` control messages
- an `[lkbridge-inbound]` custom context
- URI-encoded channel variables passed through `v(...)`

The relevant dial syntax is:

```asterisk
Dial(WebSocket/lkbridge/c(slin)nf(json)v(...),120)
```

`c(slin)` selects the media format, `n` prevents early auto-answer, `f(json)`
selects JSON control messages, and `v(...)` transports metadata.

For a parallel rollout, route only one test DID to a new custom extension and
leave the previous destination untouched as the rollback path. Apply changes
through the FreePBX UI/database and `fwconsole reload`.

## Adding another DID safely

The running bridge and its `lkbridge` WebSocket client are shared and support
concurrent per-call connections. A new DID does not need another bridge
service, listening port, credential set, or LiveKit SIP trunk.

Do not reuse a sample custom context blindly. The included example contains a
deployment-specific hardcoded `CALL_AGENT_NUMBER`; if left unchanged, a new DID
would publish incorrect metadata and might select the wrong backend agent.

Use this onboarding checklist:

1. Back up the DID's current FreePBX inbound destination.
2. Normalize the DID to the digits-only value expected by the worker and ensure
   the backend maps that number to the intended agent configuration.
3. Allocate a new unused FreePBX custom extension.
4. Copy the custom bridge context under a unique context name and set
   `CALL_AGENT_NUMBER` and `CALLEE_NUMBER` to the new DID. Continue deriving
   caller number, `UNIQUEID`, `LINKEDID`, and trace ID from the call.
5. Create a FreePBX Custom Extension targeting
   `Local/s@lkbridge-inbound-<did>/n`, or use the equivalent Custom Destination.
6. Point only the new DID's Inbound Route at the new destination and apply the
   FreePBX configuration.
7. Verify the generated route and custom context:

   ```bash
   fwconsole reload
   asterisk -rx "dialplan show <new-did>@from-trunk"
   asterisk -rx "dialplan show s@lkbridge-inbound-<did>"
   ```

8. Test canonical attributes, one agent dispatch, full-duplex audio, DTMF,
   linked-ID transfer, both hangup directions, and resource cleanup.
9. If validation fails, restore that DID's previous FreePBX destination and
   apply the configuration. Other bridge DIDs remain unchanged.

For a larger DID fleet, a shared context may derive the normalized agent number
from FreePBX's inherited `FROM_DID`. Validate every trunk's DID presentation
before adopting that pattern; dedicated contexts are safer when formats differ.

## Transfer integration

Transfers must target the original caller channel, not the WebSocket media leg.
Pass `asterisk.linkedid` to the AMI transfer endpoint and select exactly one
active channel where the channel is `PJSIP/*`, `Linkedid` equals the requested
value, and `Uniqueid == Linkedid`. Reject missing or ambiguous matches. Caller
number matching may remain as a backward-compatible fallback only when no
linked ID is available.

AMI does not need a protocol upgrade for this design; the application-level
channel-selection logic needs the linked-ID support.

## Build and install

Build with the provided container build or a compatible Go toolchain:

```bash
go test -race ./...
go build -trimpath -o asterisk-livekit-bridge .
```

Install the binary under `/opt/asterisk-livekit-bridge`, install the systemd
unit, and create a root-managed environment file:

```env
LIVEKIT_URL=wss://your-livekit-host
LIVEKIT_API_KEY=replace-me
LIVEKIT_API_SECRET=replace-me
LIVEKIT_AGENT_NAME=pentagonai
LISTEN_ADDR=127.0.0.1:8091
SETUP_TIMEOUT=20s
SHUTDOWN_TIMEOUT=10s
```

Run the service as the dedicated unprivileged `lkbridge` account. Never commit
real LiveKit or AMI credentials.

## Operations and rollback

```bash
curl -fsS http://127.0.0.1:8091/healthz
systemctl status asterisk-livekit-bridge
journalctl -u asterisk-livekit-bridge -f
asterisk -rx "core show channels concise"
ss -tnp '( sport = :8091 or dport = :8091 )'
```

After a completed call there should be no call-specific Asterisk channel or
established connection on the bridge port.

Rollback consists of returning the test DID to its previous FreePBX extension,
applying the FreePBX configuration, verifying the generated dialplan, and then
disabling the service if desired. Do not restore generated Asterisk files over
a newer FreePBX configuration.

## Validation results

Production testing verified full-duplex audio, RTC participant compatibility,
canonical metadata, single agent dispatch, DTMF events, linked-ID AMI transfer,
hangup cleanup, and absence of native LiveKit SIP in the bridge path.

A controlled two-turn test using the same DID, caller, ECS worker, agent, and
realtime model measured:

| Metric | Native LiveKit SIP | Bridge | Bridge difference |
|---|---:|---:|---:|
| Worker session startup | 1.782 s | 0.935 s | 48% faster |
| Agent ready | 2.124 s | 1.254 s | 41% faster |
| Average turn TTFT | 1.077 s | 0.633 s | 41% faster |
| Average full turn | 1.973 s | 1.560 s | 21% faster |
| Greeting completion | 14.487 s | 13.005 s | 1.482 s faster |

Observed bridge media drift was approximately 0.7-14.2 ms over 20 seconds,
with no startup frames dropped. This is a small sample, not a statistical
benchmark. `Agent greeted` measures completion of the full greeting rather
than first audible PCM.

Current committed tests cover metadata precedence, native SIP fallback, query
fallback, and identifier sanitization. Future automated coverage should add
mocked backpressure, disconnect, dispatch-race, and first-audio tests.
