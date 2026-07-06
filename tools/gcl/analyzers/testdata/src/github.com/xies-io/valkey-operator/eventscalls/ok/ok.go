package ok

import (
	"k8s.io/client-go/tools/record"

	"github.com/ioxie/velkir/internal/events"
)

func KnownReasonsAccepted(r record.EventRecorder, obj any) {
	r.Event(obj, "Normal", string(events.KnownReason), "msg")
	r.Eventf(obj, "Normal", string(events.AnotherReason), "msg %s", "fmt")
	r.AnnotatedEventf(obj, map[string]string{"a": "b"}, "Normal", string(events.AuthSecretMissing), "msg")
}

func DynamicReasonSkipped(r record.EventRecorder, obj any, dynamic string) {
	// Dynamic (non-constant) reasons are accepted today; the catalog check
	// can only validate string literals / constants.
	r.Eventf(obj, "Normal", dynamic, "msg")
}
