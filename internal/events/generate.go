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

package events

// AllReasons() is generated from the package's exported Reason consts so the
// catalog-membership list cannot drift from the declarations. `make generate`
// runs the same generator; this directive lets `go generate ./internal/events`
// refresh it directly.
//go:generate go run ../../tools/gen-reasons -src . -out zz_generated.reasons.go -header ../../tools/boilerplate.go.txt
