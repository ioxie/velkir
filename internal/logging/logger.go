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
	"github.com/go-logr/logr"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// New returns a logr.Logger that wraps controller-runtime's zap logger
// with the redaction Core driven by DefaultRegistry. Callers wire it
// into ctrl.SetLogger at manager bootstrap; every controller logger
// derived from ctrl.Log inherits the redaction pass.
//
// opts is the same crzap.Options the caller would otherwise pass to
// crzap.New (UseFlagOptions, UseDevMode, WriteTo, etc.). The redaction
// wrapper is appended via RawZapOpts so it composes with everything
// the caller already configured.
func New(opts crzap.Options) logr.Logger {
	return crzap.New(
		crzap.UseFlagOptions(&opts),
		crzap.RawZapOpts(uberzap.WrapCore(func(c zapcore.Core) zapcore.Core {
			return WrapWithRedaction(c, DefaultRegistry)
		})),
	)
}
