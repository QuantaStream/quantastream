package qsbridge

func clientPlanLifecycleKind(kind QueryKind) ClientPlanLifecycleKind {
	switch {
	case kind == QueryKindSelect:
		return ClientPlanLifecycleSelect
	case isMutationQueryKind(kind):
		return ClientPlanLifecycleMutation
	default:
		return ClientPlanLifecycleUnsupported
	}
}

func clientPlanLifecycleStepCount(kind QueryKind) int {
	switch clientPlanLifecycleKind(kind) {
	case ClientPlanLifecycleSelect:
		return 7
	case ClientPlanLifecycleMutation:
		return 7
	default:
		return 0
	}
}
