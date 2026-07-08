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

// Package gcl wires three operator-specific analyzers into a golangci-lint v2
// module plugin:
//
//   - velkir/ssa-use-helper — reject direct client.Apply / Patch
//     calls with client.Apply as the patch type. Must go through
//     internal/util/ssa.Apply (or .ApplyStatus).
//
//   - velkir/no-unbounded-client-call — reject client.List calls
//     that don't scope by namespace or label selector. Unbounded lists hit
//     the informer cache for every object of the kind in the cluster.
//
//   - velkir/events-catalog-membership — reject EventRecorder
//     Event / Eventf calls whose reason argument is not declared in
//     internal/events.Reason. Ensures every emitted reason is documented
//     and alertable.
//
// The plugin is loaded by `golangci-lint custom` via .custom-gcl.yml at the
// repo root. Invocation shape:
//
//	golangci-lint custom              # builds ./custom-gcl
//	./custom-gcl run ./...
package gcl

import (
	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"

	"github.com/ioxie/velkir/tools/gcl/analyzers"
)

func init() {
	register.Plugin("velkir-lints", New)
}

// New is golangci-lint's entrypoint: given the plugin's settings block from
// .golangci.yml (unused today), return a LinterPlugin that exposes the three
// analyzers.
func New(_ any) (register.LinterPlugin, error) {
	return &plugin{}, nil
}

type plugin struct{}

func (p *plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return []*analysis.Analyzer{
		analyzers.SSAUseHelper,
		analyzers.NoUnboundedClientCall,
		analyzers.EventsCatalogMembership,
	}, nil
}

// GetLoadMode asks golangci-lint for full type information — all three
// analyzers need pass.TypesInfo to resolve identifiers to their package
// paths (e.g. distinguishing our local Apply from client.Apply).
func (p *plugin) GetLoadMode() string {
	return register.LoadModeTypesInfo
}
