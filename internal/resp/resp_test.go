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

package resp

import "testing"

// TestEncodeCommand pins the RESP-2 wire bytes against the goldens the
// two client packages assert today: the valkey AUTH/INFO forms and the
// sentinel SENTINEL GET-MASTER-ADDR-BY-NAME form.
func TestEncodeCommand(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "valkey AUTH",
			args: []string{"AUTH", "secret"},
			want: "*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n",
		},
		{
			name: "valkey INFO replication",
			args: []string{"INFO", "replication"},
			want: "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n",
		},
		{
			name: "sentinel GET-MASTER-ADDR-BY-NAME",
			args: []string{"SENTINEL", "GET-MASTER-ADDR-BY-NAME", "vk0"},
			want: "*3\r\n$8\r\nSENTINEL\r\n$23\r\nGET-MASTER-ADDR-BY-NAME\r\n$3\r\nvk0\r\n",
		},
	} {
		if got := EncodeCommand(tc.args...); got != tc.want {
			t.Errorf("%s: EncodeCommand(%q) = %q, want %q", tc.name, tc.args, got, tc.want)
		}
	}
}

func TestMaxBulkSize(t *testing.T) {
	if MaxBulkSize != 1<<20 {
		t.Errorf("MaxBulkSize = %d, want %d", MaxBulkSize, 1<<20)
	}
}
