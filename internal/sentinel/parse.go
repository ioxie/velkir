/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sentinel

import (
	"net"
	"strconv"
	"strings"
)

// EventKind classifies a parsed sentinel pubsub message into the
// vocabulary the observer reasons about. Matches the Prometheus
// SentinelPubsubMessagesTotal `event` label values — anything not
// recognised collapses to KindOther so cardinality stays bounded.
type EventKind string

const (
	KindSwitchMaster       EventKind = "+switch-master"
	KindFailoverEnd        EventKind = "+failover-end"
	KindFailoverEndTimeout EventKind = "+failover-end-for-timeout"
	KindODown              EventKind = "+odown"
	KindODownClear         EventKind = "-odown"
	KindTilt               EventKind = "+tilt"
	KindTiltClear          EventKind = "-tilt"
	KindOther              EventKind = "other"
)

// SwitchMasterEvent is the parsed payload of `+switch-master`:
//
//	<master-name> <old-ip> <old-port> <new-ip> <new-port>
//
// All fields are required. Empty MasterName means the parser
// rejected the payload (the observer treats this as "ignore, log
// at debug, do not update snapshot").
type SwitchMasterEvent struct {
	MasterName string
	OldAddr    string
	NewAddr    string
}

// MasterEvent is the parsed payload of any `+failover-end{,-for-timeout}`
// or `+odown` / `-odown` message:
//
//	master <master-name> <ip> <port> [<other-fields>]
//
// Sentinel uses a longer form for some events ("master <name>
// <ip> <port> @ <ip> <port>" for +odown on a slave context); we
// only consume the leading 4 tokens, which are always
// `master <name> <ip> <port>`.
type MasterEvent struct {
	MasterName string
	Addr       string
}

// ParseEventKind maps a raw pubsub channel name to its EventKind.
// Unknown channels (a future Sentinel version emits a new
// notification we don't model yet) collapse to KindOther so the
// metric label stays bounded.
func ParseEventKind(channel string) EventKind {
	switch channel {
	case "+switch-master":
		return KindSwitchMaster
	case "+failover-end":
		return KindFailoverEnd
	case "+failover-end-for-timeout":
		return KindFailoverEndTimeout
	case "+odown":
		return KindODown
	case "-odown":
		return KindODownClear
	case "+tilt":
		return KindTilt
	case "-tilt":
		return KindTiltClear
	default:
		return KindOther
	}
}

// ParseSwitchMaster decodes a `+switch-master` payload. Returns
// ok=false on any malformed input (wrong field count, unparseable
// port). Caller logs at debug + ignores; the pull tick will
// re-derive the truth on the next 10s sweep.
func ParseSwitchMaster(payload string) (SwitchMasterEvent, bool) {
	fields := strings.Fields(payload)
	if len(fields) < 5 {
		return SwitchMasterEvent{}, false
	}
	oldPort, err := strconv.Atoi(fields[2])
	if err != nil || oldPort <= 0 {
		return SwitchMasterEvent{}, false
	}
	newPort, err := strconv.Atoi(fields[4])
	if err != nil || newPort <= 0 {
		return SwitchMasterEvent{}, false
	}
	return SwitchMasterEvent{
		MasterName: fields[0],
		OldAddr:    net.JoinHostPort(fields[1], strconv.Itoa(oldPort)),
		NewAddr:    net.JoinHostPort(fields[3], strconv.Itoa(newPort)),
	}, true
}

// ParseMasterEvent decodes any `master <name> <ip> <port> ...`
// payload. The leading "master " literal is what +failover-end and
// +odown share; we use this to extract the (name, addr) pair
// without caring about trailing fields.
func ParseMasterEvent(payload string) (MasterEvent, bool) {
	fields := strings.Fields(payload)
	if len(fields) < 4 || fields[0] != "master" {
		return MasterEvent{}, false
	}
	port, err := strconv.Atoi(fields[3])
	if err != nil || port <= 0 {
		return MasterEvent{}, false
	}
	return MasterEvent{
		MasterName: fields[1],
		Addr:       net.JoinHostPort(fields[2], strconv.Itoa(port)),
	}, true
}

// ParseGetMasterAddr decodes the array reply from
// SENTINEL GET-MASTER-ADDR-BY-NAME — `[<ip>, <port>]` (two bulk
// strings) on success, nil array on "no such master".
func ParseGetMasterAddr(reply any) (string, bool) {
	arr, ok := reply.([]any)
	if !ok || len(arr) < 2 {
		return "", false
	}
	host, ok := arr[0].(string)
	if !ok || host == "" {
		return "", false
	}
	portStr, ok := arr[1].(string)
	if !ok {
		return "", false
	}
	if _, err := strconv.Atoi(portStr); err != nil {
		return "", false
	}
	return net.JoinHostPort(host, portStr), true
}

// ParseSentinelMasterEpoch extracts the `config-epoch` value from a
// SENTINEL MASTER <name> reply. The reply is a flat multi-bulk
// array of alternating key/value bulk strings (keys are field names
// like "name", "ip", "port", "config-epoch", ...). We only consume
// `config-epoch`; the addr fields are read separately via
// GET-MASTER-ADDR-BY-NAME so this parser is purpose-built to drive
// ObservedPrimary.Epoch monotonicity (consumed by future stages
// for missed-failover detection).
//
// Returns ok=false on any malformed input (odd-length array,
// missing config-epoch key, non-numeric value).
func ParseSentinelMasterEpoch(reply any) (int64, bool) {
	arr, ok := reply.([]any)
	if !ok || len(arr) < 2 {
		return 0, false
	}
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		if k != "config-epoch" {
			continue
		}
		v, _ := arr[i+1].(string)
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// flagsField is the key of the comma-separated condition-flags value
// in a SENTINEL MASTER <name> reply (its tokens include s_down /
// o_down / failover_in_progress). Named once so every flat key/value
// scanner that reads it shares one literal.
const flagsField = "flags"

// MasterFlags holds the down-state tokens parsed from the `flags`
// field of a SENTINEL MASTER <name> reply. Sentinel packs the
// master's condition into one comma-separated value; SDown is this
// one sentinel's subjective-down view, ODown is the quorum-agreed
// objective-down. Only ODown can brew an election — SDown is carried
// for observability, not as a veto input.
type MasterFlags struct {
	SDown bool
	ODown bool
}

// ParseSentinelMasterFlags extracts the down-state markers from the
// `flags` field of a SENTINEL MASTER <name> reply — the same flat
// multi-bulk array of alternating key/value bulk strings that
// ParseSentinelMasterEpoch reads. The `flags` value is a
// comma-separated token list ("master", "master,s_down",
// "master,s_down,o_down", ...).
//
// Returns ok=false on any input a caller must treat as "no usable
// observation this tick" — a nil / short / odd-length array, a
// missing `flags` key, or a non-string `flags` value — so a pull tick
// that could not read the field leaves its pull-side map untouched
// (absence of evidence is not evidence of absence). Returns ok=true
// with both markers false for a healthy, present-and-clear `master`:
// the discriminator a caller uses to authorise a pull-confirmed clear.
func ParseSentinelMasterFlags(reply any) (MasterFlags, bool) {
	v, ok := masterReplyFlags(reply)
	if !ok {
		return MasterFlags{}, false
	}
	var f MasterFlags
	for tok := range strings.SplitSeq(v, ",") {
		switch tok {
		case "s_down":
			f.SDown = true
		case "o_down":
			f.ODown = true
		}
	}
	return f, true
}

// ParseSentinelMasterAuthPass extracts the `auth-pass` value
// from a SENTINEL MASTER <name> reply. Sentinel returns the
// auth-pass field as a literal echo of what was set via
// `SENTINEL SET <name> auth-pass <password>` — used by the
// verification round-trip to confirm the SET landed on this
// specific sentinel pod.
//
// Returns ok=false on any malformed input (odd-length array,
// missing auth-pass key); ok=true with empty string when the
// key is present but the value is blank — sentinel returns ""
// for an unset auth-pass, which the caller treats as "not yet
// set" and distinct from "set to empty string" (which the
// operator never does because empty password skips propagation
// upstream).
func ParseSentinelMasterAuthPass(reply any) (string, bool) {
	arr, ok := reply.([]any)
	if !ok || len(arr) < 2 {
		return "", false
	}
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		if k != "auth-pass" {
			continue
		}
		v, _ := arr[i+1].(string)
		return v, true
	}
	return "", false
}

// ParseSentinelMasterNumOtherSentinels extracts the
// `num-other-sentinels` value from a SENTINEL MASTER <name> reply.
// Used by the wedge-recovery read-back path to confirm a freshly-
// RESET'd sentinel has rebuilt its peer-list via gossip — empty
// peer-list after operator-driven RESET + MONITOR is the canonical
// stranded-sentinel signature.
//
// Returns ok=false on any malformed input (odd-length array,
// missing num-other-sentinels key, non-numeric value).
func ParseSentinelMasterNumOtherSentinels(reply any) (int, bool) {
	arr, ok := reply.([]any)
	if !ok || len(arr) < 2 {
		return 0, false
	}
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		if k != "num-other-sentinels" {
			continue
		}
		v, _ := arr[i+1].(string)
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// ParseSentinelMasterNumSlaves extracts the `num-slaves` value from a
// SENTINEL MASTER <name> reply — the number of replicas the sentinel
// knows about for the master. Mirrors ParseSentinelMasterNumOtherSentinels
// exactly (same flat key/value scan, same guards); read from the same
// reply on the pull tick so the topology-hygiene signal costs no extra
// round-trip. It may later be superseded by a unified SENTINEL MASTER
// field parse that reads every count field in one scan.
//
// Returns ok=false on any malformed input (odd-length array, missing
// num-slaves key, non-numeric value).
func ParseSentinelMasterNumSlaves(reply any) (int, bool) {
	arr, ok := reply.([]any)
	if !ok || len(arr) < 2 {
		return 0, false
	}
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		if k != "num-slaves" {
			continue
		}
		v, _ := arr[i+1].(string)
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// ParseCKQuorum decodes the simple-string reply from
// SENTINEL CKQUORUM — `+OK <n> usable Sentinels. Quorum and ...`
// on success (returns ok=true). Anything else (including the
// `-NOQUORUM` error sentinel returns) is ok=false.
//
// Caller doesn't care about the message body; the boolean is
// the gate for QuorumOK in the observed snapshot.
func ParseCKQuorum(reply any) bool {
	if err, isErr := reply.(error); isErr && err != nil {
		return false
	}
	s, ok := reply.(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(s, "OK ") || s == "OK"
}
