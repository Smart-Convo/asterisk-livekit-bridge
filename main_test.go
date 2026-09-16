package main

import (
	"encoding/json"
	"net/url"
	"testing"
)

func TestMetadataCanonicalAndCompatibilityAliases(t *testing.T) {
	start := mediaStart{ChannelID: "171.2", ChannelVariables: map[string]string{
		"ASTERISK_UNIQUEID": "171.2", "ASTERISK_LINKEDID": "171.1",
		"CALLER_NUMBER": "+92 333-1251264", "CALLEE_NUMBER": "+924232460261",
		"CALL_TRACE_ID": "trace-1",
	}}
	meta, attrs := metadataFrom(start, url.Values{})
	if meta.HumanNumber != "923331251264" || meta.AgentNumber != "924232460261" { t.Fatalf("unexpected metadata: %+v", meta) }
	checks := map[string]string{
		"call.human_number": "923331251264", "call.agent_number": "924232460261",
		"call.type": "Inbound", "call.channel": "PBX", "asterisk.linkedid": "171.1",
		"sip.phoneNumber": "923331251264", "sip.trunkPhoneNumber": "924232460261",
	}
	for key, want := range checks { if attrs[key] != want { t.Errorf("%s=%q want %q", key, attrs[key], want) } }
	if _, err := json.Marshal(meta); err != nil { t.Fatal(err) }
}

func TestMetadataQueryFallback(t *testing.T) {
	query := url.Values{
		"asterisk_linkedid": {"call.9"}, "caller_number": {"03001234567"},
		"callee_number": {"32460261"},
	}
	meta, _ := metadataFrom(mediaStart{ChannelID: "call.10"}, query)
	if meta.LinkedID != "call.9" || meta.UniqueID != "call.10" || meta.HumanNumber != "03001234567" { t.Fatalf("unexpected: %+v", meta) }
}

func TestSafeID(t *testing.T) {
	if got := safeID("171.2/+x"); got != "171_2__x" { t.Fatalf("got %q", got) }
}
