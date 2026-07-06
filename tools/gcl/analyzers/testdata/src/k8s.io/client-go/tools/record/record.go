// Package record is a minimal stub of k8s.io/client-go/tools/record for
// analysistest. Real imports are replaced by this skeleton.
package record

type EventRecorder interface {
	Event(object any, eventtype, reason, message string)
	Eventf(object any, eventtype, reason, messageFmt string, args ...any)
	AnnotatedEventf(object any, annotations map[string]string, eventtype, reason, messageFmt string, args ...any)
}
