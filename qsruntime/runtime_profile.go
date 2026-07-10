package qsruntime

// RuntimeImplementation names the concrete runtime implementation behind a route.
type RuntimeImplementation string

const (
	// RuntimeImplementationLegacyDirect identifies the source.NewQuantaSource direct adapter.
	RuntimeImplementationLegacyDirect RuntimeImplementation = "legacy_direct"
	// RuntimeImplementationFixture identifies the deterministic in-memory runtime fixture.
	RuntimeImplementationFixture RuntimeImplementation = "runtime_fixture"
)

// RuntimeInspectionProfile describes the concrete runtime inspected for a request.
type RuntimeInspectionProfile struct {
	Implementation RuntimeImplementation
	Detail         string
}

// LegacyDirectRuntimeProfile returns the default profile for the real direct legacy adapter.
func LegacyDirectRuntimeProfile() RuntimeInspectionProfile {
	return RuntimeInspectionProfile{Implementation: RuntimeImplementationLegacyDirect}
}

// FixtureRuntimeProfile returns a profile for deterministic fixture-backed runtimes.
func FixtureRuntimeProfile(detail string) RuntimeInspectionProfile {
	return RuntimeInspectionProfile{Implementation: RuntimeImplementationFixture, Detail: detail}
}

// Effective returns a default profile when none was configured.
func (p RuntimeInspectionProfile) Effective() RuntimeInspectionProfile {
	if p.Implementation == "" {
		return LegacyDirectRuntimeProfile()
	}
	return p
}
