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

// Package resp holds the byte-identical RESP-2 wire-format primitives
// shared by the operator's direct-Valkey and Sentinel clients. It is
// stdlib-only and imports no operator package, so both client packages
// can depend on it without an import cycle.
package resp

import (
	"strconv"
	"strings"
)

// MaxBulkSize bounds the byte allocation a RESP-2 bulk-string reader is
// willing to do for one reply. Sized well above any realistic reply so
// the operator cannot be pushed into a single-allocation OOM by a
// compromised or wedged peer that responds with `$<huge>\r\n`.
const MaxBulkSize = 1 << 20 // 1 MiB

// EncodeCommand renders one RESP-2 command in array-of-bulk-string form
// (`*N\r\n$L\r\n<arg>\r\n`...). The encoding is the minimal subset the
// operator's clients need — no pipelining or RESP-3 features.
func EncodeCommand(args ...string) string {
	var b strings.Builder
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(a)))
		b.WriteString("\r\n")
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	return b.String()
}
