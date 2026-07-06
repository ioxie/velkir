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

// Package events is the canonical catalog of Kubernetes Event reasons the
// operator emits. Every recorder.Eventf / record.Eventf / similar call site
// MUST pass a Reason declared here — the velkir/events-catalog-membership
// linter enforces membership so undocumented reasons can't slip in.
//
// The analyzer walks every exported const of type `Reason` in this package.
// Plain untyped string consts are NOT picked up; declare every reason as
// `Reason` so the linter sees it.
//
// Every addition here should come with a one-line doc comment explaining
// when the reason is emitted, plus an alert-rule reference (or a
// conscious decision that the reason is informational and non-alertable).
package events

// Reason is the string type used for the `reason` field of emitted events.
// Declaring constants as Reason instead of plain string makes misuse
// (passing a typo'd raw string) a compile error and makes the
// events-catalog-membership linter's picker exact.
type Reason string
