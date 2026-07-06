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

// Package orchestration bundles the FSM-driving components of the
// reconciler: the per-condition status evaluators, the master-aware
// rollout state machine, and the per-pod readiness watchdog. They are
// grouped together because the reconciler drives them as one unit each
// tick — the watchdog verdict feeds the status evaluation, status
// observations feed the FSM guards, and the FSM's side effects feed
// back into the next status pass.
//
// Status evaluators (one file per condition: available.go, degraded.go,
// progressing.go, etc.) reduce a CR's observed state down to a
// `[]metav1.Condition` and a derived `phase` string. Each condition
// evolves independently as new failure modes are discovered.
//
// The rollout FSM (states.go, events.go, transitions.go) is intentionally
// pure: it consumes abstract guard inputs (quorum status, replica
// readiness, observed failover events) and emits abstract side effects
// (event reasons, requeue durations, condition updates). It does not
// import controller-runtime, k8s.io/api, or any reconciler types so the
// transition table can be exhaustively tested without an envtest. The
// reconciler is the integrator: derives the current State from observed
// cluster state, populates a GuardCtx from the same observation, calls
// Machine.Apply with the appropriate Event, and translates the returned
// SideEffect into concrete reconciler actions. State / Event / Transition
// shapes mirror the spec exactly — adding a transition means adding a
// row to the table in transitions.go and a test case in
// transitions_test.go. The state enum is closed; new states require a
// coordinated update across both files.
//
// The rollout watchdog (watchdog.go) is a pure helper: takes a clock-now
// value, the persisted MasterAwareRolloutStatus substate, and the
// configured timeout, and reports whether the watchdog has expired. The
// caller is responsible for reading current cluster state (is the
// replacement pod Ready yet?), invoking the helper to decide whether to
// declare a stall, and emitting the corresponding RolloutStalled event +
// setting the Degraded condition. Pure-function shape lets the watchdog
// be tested deterministically against a synthetic clock.
package orchestration
