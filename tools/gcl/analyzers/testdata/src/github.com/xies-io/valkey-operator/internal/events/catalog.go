// Package events is a stub of the operator's events catalog for analysistest.
// The real package declares exported string consts of type Reason.
package events

type Reason string

// Known reasons exercised by the events-catalog-membership test. All three
// are typed as Reason so the analyzer picks them up; anything untyped
// should be ignored.
const (
	KnownReason       Reason = "KnownReason"
	AnotherReason     Reason = "AnotherReason"
	AuthSecretMissing Reason = "AuthSecretMissing"

	// Untyped string — the analyzer must NOT consider this a catalog entry.
	NotAReason = "ShouldBeIgnoredByAnalyzer"
)
