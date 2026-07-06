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

// Package ssa wraps controller-runtime's Server-Side Apply so every operator
// write goes through a single chokepoint that:
//
//  1. Stamps the single FieldOwner the operator uses for all of its writes.
//  2. Routes through the typed apply-configuration API
//     (Client.Apply(runtime.ApplyConfiguration, ...) — no DeepCopy or
//     managedFields stripping needed because apply-configurations are
//     freshly-built builders, not caller-owned objects pulled from the cache.
//
// Direct client.Apply / client.Patch(..., client.Apply, ...) calls elsewhere
// in the codebase are forbidden; the FieldOwner stamping is too easy to
// forget. The custom linter velkir/ssa-use-helper makes the ban
// mechanical.
package ssa

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FieldOwner is the SSA field manager the operator uses for all reconciler
// writes. A distinct owner (e.g. velkir-ca-injector) is expected for
// subsystems that compete for the same fields; callers pass their own
// client.FieldOwner in opts to override the default.
const FieldOwner = client.FieldOwner("velkir")
