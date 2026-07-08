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
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak.VerifyTestMain so any test in this package
// that leaves goroutines behind after returning (a dropped
// wg.Done(), an unwired ctx, a forgotten cancel) fails the suite
// with a stack-trace dump of the leaked goroutine.
//
// Justification: the package
// spawns four distinct kinds of goroutines per observer (PSUBSCRIBE
// loop per endpoint, watcher-goroutine for TCP-close-on-ctx-cancel,
// poll loop, parallel queryOne workers in pollOnce) plus the
// Manager's Start-loop. The lifecycle tests assert that
// Manager.Start returns within a bounded time after ctx-cancel and
// that observer.stop drains, but those are presence checks — they
// don't catch a future regression that drops a wg.Done() or
// forgets to plumb ctx into a new goroutine. goleak structurally
// catches all three classes; the cost is one TestMain per package.
//
// No IgnoreTopFunction filters needed: this package doesn't import
// controller-runtime or any other library that leaves housekeeping
// goroutines behind. If a future change adds one (e.g. wiring the
// controller's manager.Manager into a sentinel test), prefer
// per-test `defer goleak.VerifyNone(t, IgnoreTopFunction(...))` to
// scoping the ignore narrowly.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
