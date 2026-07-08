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
	"slices"
	"sync"
	"testing"
)

func TestRegistry_RegisterAndSnapshot(t *testing.T) {
	r := NewRegistry()
	r.Register("supersecret-1234")
	r.Register("another-secret-5678")

	snap := r.Snapshot()
	if got := len(snap); got != 2 {
		t.Fatalf("snapshot len = %d, want 2", got)
	}
	if got := r.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestRegistry_DropsTooShortTokens(t *testing.T) {
	r := NewRegistry()
	for _, short := range []string{"", "a", "abc", "1234567"} { // all under MinTokenLen
		r.Register(short)
	}
	if r.Len() != 0 {
		t.Fatalf("registry accepted tokens shorter than MinTokenLen=%d: snapshot=%v", MinTokenLen, r.Snapshot())
	}
}

func TestRegistry_RefcountedForget(t *testing.T) {
	r := NewRegistry()
	const tok = "shared-secret-xyz"
	r.Register(tok)
	r.Register(tok)
	r.Register(tok)

	r.Forget(tok)
	if r.Len() != 1 {
		t.Fatalf("forget after 3 registers + 1 forget should leave token in place; len=%d", r.Len())
	}
	r.Forget(tok)
	if r.Len() != 1 {
		t.Fatalf("forget after 3 registers + 2 forgets should leave token in place; len=%d", r.Len())
	}
	r.Forget(tok)
	if r.Len() != 0 {
		t.Fatalf("forget that drives count to zero should evict; len=%d", r.Len())
	}
}

func TestRegistry_ForgetUnknown(t *testing.T) {
	r := NewRegistry()
	r.Forget("never-registered-1234") // must not panic
	r.Register("known-token-12345678")
	r.Forget("known-token-12345678")
	r.Forget("known-token-12345678") // double forget on already-evicted
	if r.Len() != 0 {
		t.Fatalf("double forget should not corrupt state; len=%d", r.Len())
	}
}

func TestRegistry_ConcurrentRegisterAndSnapshot(t *testing.T) {
	r := NewRegistry()
	const writers = 8
	const perWriter = 250
	var wg sync.WaitGroup
	wg.Add(writers + 1)

	tokens := []string{
		"alpha-secret-aaaaaaaa",
		"bravo-secret-bbbbbbbb",
		"charlie-secret-cccccccc",
		"delta-secret-dddddddd",
	}

	for range writers {
		go func() {
			defer wg.Done()
			for i := range perWriter {
				t := tokens[i%len(tokens)]
				r.Register(t)
				r.Forget(t)
			}
		}()
	}
	go func() {
		defer wg.Done()
		for range perWriter * 4 {
			_ = r.Snapshot()
		}
	}()

	wg.Wait()
	if r.Len() != 0 {
		t.Fatalf("matched register/forget pairs should leave registry empty; len=%d, snapshot=%v",
			r.Len(), r.Snapshot())
	}
}

func TestRegistry_RegisterScoped(t *testing.T) {
	r := NewRegistry()
	const tok = "scoped-secret-12345678"

	cleanup := r.RegisterScoped(tok)
	if r.Len() != 1 {
		t.Fatalf("RegisterScoped did not register; len=%d", r.Len())
	}
	cleanup()
	if r.Len() != 0 {
		t.Fatalf("cleanup did not Forget; len=%d", r.Len())
	}
}

func TestRegistry_RegisterScoped_NoOpForShortOrEmpty(t *testing.T) {
	r := NewRegistry()
	for _, short := range []string{"", "abc", "1234567"} {
		cleanup := r.RegisterScoped(short)
		if r.Len() != 0 {
			t.Fatalf("RegisterScoped accepted token shorter than MinTokenLen=%d (%q)", MinTokenLen, short)
		}
		cleanup() // must be safe to call even when nothing was registered
	}
	if r.Len() != 0 {
		t.Fatalf("registry mutated by short-token cleanups; len=%d", r.Len())
	}
}

func TestRegistry_RegisterScoped_RefcountPair(t *testing.T) {
	r := NewRegistry()
	const tok = "shared-scoped-secret-abcdefgh"

	c1 := r.RegisterScoped(tok)
	c2 := r.RegisterScoped(tok)
	if r.Len() != 1 {
		t.Fatalf("two RegisterScoped calls should refcount one entry; len=%d", r.Len())
	}
	c1()
	if r.Len() != 1 {
		t.Fatalf("first cleanup should leave token registered; len=%d", r.Len())
	}
	c2()
	if r.Len() != 0 {
		t.Fatalf("second cleanup should evict; len=%d", r.Len())
	}
}

func TestRegistry_DefaultRegistryIsUsable(t *testing.T) {
	// Smoke-test the package singleton stays addressable; reset state we
	// touch so other tests aren't poisoned.
	const tok = "default-registry-probe-secret-abc"
	DefaultRegistry.Register(tok)
	defer DefaultRegistry.Forget(tok)

	if !slices.Contains(DefaultRegistry.Snapshot(), tok) {
		t.Fatalf("DefaultRegistry snapshot did not include just-registered token")
	}
}
