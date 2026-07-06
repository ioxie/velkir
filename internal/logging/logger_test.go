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

package logging

import (
	"bytes"
	"strings"
	"testing"

	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// TestNew_RedactsViaDefaultRegistry pins the production wireup of the
// New() factory: callers get a logr.Logger whose output passes through
// the redacting Core driven by the package's DefaultRegistry. The
// other tests in this package build the redacting Core via the local
// buildRedactingTestLogger helper, so they cover the redactor's
// behaviour but not the New() function's RawZapOpts/WrapCore plumbing.
// A future refactor of New() — swapping DefaultRegistry for a
// constructor-injected registry, dropping RawZapOpts, reordering
// WrapCore — could silently break production redaction without any
// existing test failing. This test trips on that change.
func TestNew_RedactsViaDefaultRegistry(t *testing.T) {
	const password = "factory-redaction-secret-fritz-lang-1931"

	DefaultRegistry.Register(password)
	t.Cleanup(func() { DefaultRegistry.Forget(password) })

	var buf bytes.Buffer
	log := New(crzap.Options{DestWriter: &buf})

	log.Info("AUTH " + password + " rejected")

	out := buf.String()

	if strings.Contains(out, password) {
		t.Errorf("logging.New() did not wire the redacting Core through DefaultRegistry; "+
			"raw password leaked into output:\n%s", out)
	}

	// Sanity: the placeholder must appear, otherwise the test passes
	// for the wrong reason (empty buffer because the logger swallowed
	// the emission, level filtered it out, etc.).
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("redaction placeholder %q never appeared; logger was not exercised:\n%s",
			RedactedPlaceholder, out)
	}
}
