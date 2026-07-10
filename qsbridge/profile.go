package qsbridge

// ExecutionTiming is one named duration reported by a future executor.
type ExecutionTiming struct {
	Name    string
	Elapsed int64
	Unit    string
}

// ExecutionCounter is one named counter reported by a future executor.
type ExecutionCounter struct {
	Name  string
	Value uint64
	Unit  string
}

// ExecutionProfile is protocol-neutral explain/profile metadata for one request.
//
// qsbridge can populate explain text from planning artifacts. Runtime timings
// and counters are placeholders for future executors to fill.
type ExecutionProfile struct {
	RequestID      ExecutionRequestID
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	TraceExplain   bool
	IncludeProfile bool
	LogicalPlan    string
	PhysicalPlan   string
	Timings        []ExecutionTiming
	Counters       []ExecutionCounter
	Diagnostics    DiagnosticSet
}

// Empty reports whether the profile has no requested or collected metadata.
func (p ExecutionProfile) Empty() bool {
	return !p.TraceExplain &&
		!p.IncludeProfile &&
		p.LogicalPlan == "" &&
		p.PhysicalPlan == "" &&
		len(p.Timings) == 0 &&
		len(p.Counters) == 0 &&
		len(p.Diagnostics) == 0
}

// ExecutionProfile returns explain/profile metadata requested by execution options.
func (r ExecutionRequest) ExecutionProfile() ExecutionProfile {
	return newExecutionProfile(r.Options, r.Bound.Prepared)
}

// ExecutionProfile returns explain/profile metadata requested by batch execution options.
func (r BatchExecutionRequest) ExecutionProfile() ExecutionProfile {
	return newExecutionProfile(r.Options, r.Prepared)
}

func newExecutionProfile(options ExecutionOptions, prepared PreparedPlan) ExecutionProfile {
	profile := ExecutionProfile{
		RequestID:      options.RequestID,
		AccessIntent:   prepared.AccessIntent(),
		Lifecycle:      clientPlanLifecycleKind(prepared.Kind),
		LifecycleSteps: clientPlanLifecycleStepCount(prepared.Kind),
		TraceExplain:   options.TraceExplain,
		IncludeProfile: options.IncludeProfile,
	}
	if options.TraceExplain {
		profile.LogicalPlan = prepared.Inspection.Logical.Text()
		profile.PhysicalPlan = prepared.Inspection.Physical.Text()
		profile.Diagnostics = cloneDiagnosticSet(prepared.Inspection.Diagnostics)
	}
	return profile
}

func cloneExecutionProfile(profile ExecutionProfile) ExecutionProfile {
	profile.Timings = append([]ExecutionTiming(nil), profile.Timings...)
	profile.Counters = append([]ExecutionCounter(nil), profile.Counters...)
	profile.Diagnostics = cloneDiagnosticSet(profile.Diagnostics)
	return profile
}
