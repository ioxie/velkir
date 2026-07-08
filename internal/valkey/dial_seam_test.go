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

package valkey

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// errInjectedDial is the sentinel an errDialer returns. A fake-dialer
// test asserts it propagates out of the issuer, which proves the issuer
// routed its dial through the injected contextDialer seam — the sentinel
// can surface no other way (a real net.Dialer would fail differently).
var errInjectedDial = errors.New("injected-dial-sentinel")

// errDialer is a contextDialer whose DialContext always fails with
// errInjectedDial, the minimal fake that exercises the injection seam.
type errDialer struct{}

func (errDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	return nil, errInjectedDial
}

const seamTestAddr = "10.7.7.7:6379"

func TestDialingReplicaOfIssuerUsesInjectedDialer(t *testing.T) {
	d := &DialingReplicaOfIssuer{Timeout: time.Second, dialer: errDialer{}}
	err := d.IssueReplicaOf(context.Background(), seamTestAddr, "", "10.7.7.8", DefaultPort)
	if !errors.Is(err, errInjectedDial) {
		t.Errorf("IssueReplicaOf err = %v, want it routed through the injected dialer", err)
	}
}

func TestDialingClientKillIssuerUsesInjectedDialer(t *testing.T) {
	d := &DialingClientKillIssuer{Timeout: time.Second, dialer: errDialer{}}
	_, err := d.KillNormalClients(context.Background(), seamTestAddr, "")
	if !errors.Is(err, errInjectedDial) {
		t.Errorf("KillNormalClients err = %v, want it routed through the injected dialer", err)
	}
}

func TestRotateOneUsesInjectedDialer(t *testing.T) {
	err := rotateOne(context.Background(), Endpoint{Name: "mst", Addr: seamTestAddr}, "", "new", errDialer{})
	if !errors.Is(err, errInjectedDial) {
		t.Errorf("rotateOne err = %v, want it routed through the injected dialer", err)
	}
}
