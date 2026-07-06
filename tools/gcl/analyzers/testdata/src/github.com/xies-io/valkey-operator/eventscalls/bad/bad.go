package bad

import (
	"k8s.io/client-go/tools/record"

	// Import the events catalog so the analyzer sees it in pass.Pkg.Imports().
	// Without this, the catalog resolves to empty and the analyzer disables
	// enforcement.
	"github.com/ioxie/velkir/internal/events"
)

func UndeclaredReasonFlagged(r record.EventRecorder, obj any) {
	r.Eventf(obj, "Normal", "TotallyMadeUpReason", "msg %s", "x") // want `event reason TotallyMadeUpReason is not declared`
}

// UntypedStringNotCatalog verifies the "Reason-typed-only" rule: an untyped
// string const from the events package is NOT treated as a catalog entry.
func UntypedStringNotCatalog(r record.EventRecorder, obj any) {
	r.Eventf(obj, "Normal", events.NotAReason, "msg") // want `event reason ShouldBeIgnoredByAnalyzer is not declared`
}
