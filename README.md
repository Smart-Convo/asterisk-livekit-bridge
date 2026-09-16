# Asterisk–LiveKit bridge

This service accepts Asterisk 20 `chan_websocket` media on localhost and joins
LiveKit as a normal RTC participant. Asterisk owns SIP, RTP, codecs, pacing,
DTMF, call routing, and transfers. The bridge only moves 8-kHz mono PCM16
between Asterisk and the LiveKit Go SDK.

The dialplan uses the `chan_websocket` `n` option to suppress automatic
answering. The bridge drains stale startup media while LiveKit connects and
sends Asterisk `ANSWER` only when the first agent PCM sample is ready. This
keeps the PSTN call ringing during agent startup instead of answering into
silence or replaying queued audio late.

Bridge participants use `call.channel=PBX` and join as normal RTC participants.
Canonical `call.*` and `asterisk.*` attributes are published alongside temporary
`sip.*` compatibility aliases.

Required environment variables:

```env
LIVEKIT_URL=wss://example.livekit.host
LIVEKIT_API_KEY=...
LIVEKIT_API_SECRET=...
LIVEKIT_AGENT_NAME=pentagonai
LISTEN_ADDR=127.0.0.1:8091
```

Endpoints are `GET /healthz` and WebSocket `/media` with subprotocol `media`.

See [`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md) for the architecture,
media and metadata contracts, dependency list, FreePBX deployment, transfer
behavior, latency correction, production results, operations, and rollback.
