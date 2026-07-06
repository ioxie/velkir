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

import "sync"

// MinTokenLen is the floor for tokens accepted by Registry.Register. Tokens
// shorter than this are silently dropped — at 7 chars or fewer the false-
// positive rate of substring matches against ordinary log content (UUIDs,
// short identifiers, base64 fragments) starts producing aggressively
// over-redacted output that hides operational signal. Operators running
// genuinely-short Secret values get an unredacted log surface for those
// values; the operator-of-the-operator audit trail plus the Secret
// length warning event is the right channel for that configuration
// smell, not silently scrubbing every 4-char string in every log line.
const MinTokenLen = 8

// RedactedPlaceholder is the literal substituted in place of every
// registered token. Constant value (not configurable) so log scrubbing
// produces stable diffs in tests and operators learn one shape.
const RedactedPlaceholder = "[REDACTED]"

// Registry tracks the active set of tokens the redacting Core scrubs from
// log emissions. The refcount lets two Valkey CRs share a Secret value
// without one CR's deletion clearing the other's protection.
type Registry struct {
	mu     sync.RWMutex
	counts map[string]int
}

// NewRegistry returns an empty Registry ready for Register/Forget calls.
func NewRegistry() *Registry {
	return &Registry{counts: map[string]int{}}
}

// Register adds token to the set. Calls are refcounted — N Register calls
// must be matched by N Forget calls before the token is actually evicted.
// Tokens shorter than MinTokenLen are silently dropped (see the constant's
// doc for why).
func (r *Registry) Register(token string) {
	if len(token) < MinTokenLen {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[token]++
}

// RegisterScoped registers token for the duration of the calling
// scope. Returns a cleanup func the caller MUST defer-call to release
// the registration. Tokens shorter than MinTokenLen are silently
// dropped (matching Register's contract); the returned cleanup is a
// no-op in that case so callers don't need to special-case empty or
// short values at every call site.
func (r *Registry) RegisterScoped(token string) func() {
	if len(token) < MinTokenLen {
		return func() {}
	}
	r.Register(token)
	return func() { r.Forget(token) }
}

// Forget decrements token's refcount and evicts it when the count hits
// zero. Forgetting an unknown token is a no-op; Forget on a too-short
// token is also a no-op (Register would have dropped it).
func (r *Registry) Forget(token string) {
	if len(token) < MinTokenLen {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counts[token]
	if !ok {
		return
	}
	if c <= 1 {
		delete(r.counts, token)
		return
	}
	r.counts[token] = c - 1
}

// Snapshot returns a point-in-time copy of the registered tokens. The
// caller may iterate freely; concurrent Register/Forget calls don't affect
// the returned slice. Returns nil when the registry is empty so the
// redactor can short-circuit without allocating.
func (r *Registry) Snapshot() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.counts) == 0 {
		return nil
	}
	out := make([]string, 0, len(r.counts))
	for t := range r.counts {
		out = append(out, t)
	}
	return out
}

// Len returns the current number of distinct tokens. Useful for tests
// and a future cardinality metric.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.counts)
}

// DefaultRegistry is the process-wide singleton wired into the redacting
// Core constructed by New(). Reconcilers should call DefaultRegistry.Register
// after reading a Secret value and DefaultRegistry.Forget when the value
// is no longer in scope (CR deletion, Secret rotation supersedes the prior
// value).
var DefaultRegistry = NewRegistry()
