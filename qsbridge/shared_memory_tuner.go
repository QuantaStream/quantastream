package qsbridge

import "math"

// This file contains a deterministic shared-memory sizing controller. The
// controller observes bounded shared-memory stats and proposes size changes, but
// it does not run timers, spawn goroutines, or mutate caches directly.

// sharedMemoryTuningAction names one sizing decision from the controller.
type sharedMemoryTuningAction string

const (
	sharedMemoryTuningHold   sharedMemoryTuningAction = "hold"
	sharedMemoryTuningGrow   sharedMemoryTuningAction = "grow"
	sharedMemoryTuningShrink sharedMemoryTuningAction = "shrink"
)

// sharedMemoryTunerConfig controls conservative shared-memory resizing.
type sharedMemoryTunerConfig struct {
	Enabled        bool
	MinEntries     int
	MaxEntries     int
	TargetHitRatio float64
	MinimumLookups uint64
	GrowPercent    float64
	ShrinkPercent  float64
}

// sharedMemoryTunerState records the previous observation for interval deltas.
type sharedMemoryTunerState struct {
	initialized              bool
	previousHits             uint64
	previousMisses           uint64
	previousEvictions        uint64
	previousLimit            int
	previousIntervalHitRatio float64
}

// sharedMemoryTuningDecision is the result of one stats observation.
type sharedMemoryTuningDecision struct {
	Action            sharedMemoryTuningAction
	Reason            string
	CurrentEntries    int
	CurrentLimit      int
	NextLimit         int
	IntervalHits      uint64
	IntervalMisses    uint64
	IntervalEvictions uint64
	IntervalHitRatio  float64
	TargetHitRatio    float64
}

// sharedMemoryTuner computes bounded shared-memory resizing decisions.
type sharedMemoryTuner struct {
	config sharedMemoryTunerConfig
	state  sharedMemoryTunerState
}

// newSharedMemoryTuner creates a deterministic tuner with normalized defaults.
func newSharedMemoryTuner(config sharedMemoryTunerConfig) *sharedMemoryTuner {
	return &sharedMemoryTuner{config: normalizeSharedMemoryTunerConfig(config)}
}

// Observe records stats and returns the next recommended shared-memory size.
func (t *sharedMemoryTuner) Observe(stats shardedValueCacheStats) sharedMemoryTuningDecision {
	if t == nil {
		return sharedMemoryTuningDecision{Action: sharedMemoryTuningHold, Reason: "nil_tuner"}
	}
	config := t.config
	decision := sharedMemoryTuningDecision{
		Action:         sharedMemoryTuningHold,
		Reason:         "hold",
		CurrentEntries: stats.Entries,
		CurrentLimit:   stats.MaxEntries,
		NextLimit:      stats.MaxEntries,
		TargetHitRatio: config.TargetHitRatio,
	}
	defer t.remember(stats, decision)

	if !config.Enabled {
		decision.Reason = "disabled"
		return decision
	}
	if stats.MaxEntries < 0 {
		decision.Reason = "unbounded"
		return decision
	}
	if !t.state.initialized {
		decision.Reason = "baseline"
		return decision
	}
	if stats.Hits < t.state.previousHits || stats.Misses < t.state.previousMisses || stats.Evictions < t.state.previousEvictions {
		decision.Reason = "counter_reset"
		return decision
	}

	decision.IntervalHits = stats.Hits - t.state.previousHits
	decision.IntervalMisses = stats.Misses - t.state.previousMisses
	decision.IntervalEvictions = stats.Evictions - t.state.previousEvictions
	lookups := decision.IntervalHits + decision.IntervalMisses
	if lookups < config.MinimumLookups {
		decision.Reason = "insufficient_samples"
		return decision
	}
	decision.IntervalHitRatio = float64(decision.IntervalHits) / float64(lookups)

	if decision.IntervalHitRatio < config.TargetHitRatio {
		if stats.MaxEntries >= config.MaxEntries {
			decision.Reason = "below_target_at_max"
			return decision
		}
		decision.Action = sharedMemoryTuningGrow
		decision.Reason = "below_target"
		if decision.IntervalEvictions > 0 {
			decision.Reason = "eviction_pressure"
		}
		decision.NextLimit = clampInt(stats.MaxEntries+sharedMemoryTuningStep(stats.MaxEntries, config.GrowPercent), config.MinEntries, config.MaxEntries)
		return decision
	}

	if stats.MaxEntries <= config.MinEntries {
		decision.Reason = "target_met_at_min"
		return decision
	}
	if stats.MaxEntries > t.state.previousLimit && decision.IntervalHitRatio <= t.state.previousIntervalHitRatio {
		decision.Action = sharedMemoryTuningShrink
		decision.Reason = "growth_no_benefit"
		decision.NextLimit = clampInt(stats.MaxEntries-sharedMemoryTuningStep(stats.MaxEntries, config.ShrinkPercent), config.MinEntries, config.MaxEntries)
		return decision
	}
	if decision.IntervalHitRatio > config.TargetHitRatio+0.05 && decision.IntervalEvictions == 0 {
		decision.Action = sharedMemoryTuningShrink
		decision.Reason = "above_target"
		decision.NextLimit = clampInt(stats.MaxEntries-sharedMemoryTuningStep(stats.MaxEntries, config.ShrinkPercent), config.MinEntries, config.MaxEntries)
		return decision
	}

	decision.Reason = "target_met"
	return decision
}

func (t *sharedMemoryTuner) remember(stats shardedValueCacheStats, decision sharedMemoryTuningDecision) {
	t.state.initialized = true
	t.state.previousHits = stats.Hits
	t.state.previousMisses = stats.Misses
	t.state.previousEvictions = stats.Evictions
	t.state.previousLimit = stats.MaxEntries
	t.state.previousIntervalHitRatio = decision.IntervalHitRatio
}

func normalizeSharedMemoryTunerConfig(config sharedMemoryTunerConfig) sharedMemoryTunerConfig {
	if config.TargetHitRatio <= 0 || config.TargetHitRatio > 1 {
		config.TargetHitRatio = 0.80
	}
	if config.MinimumLookups == 0 {
		config.MinimumLookups = 1
	}
	if config.GrowPercent <= 0 {
		config.GrowPercent = 0.20
	}
	if config.ShrinkPercent <= 0 {
		config.ShrinkPercent = 0.10
	}
	if config.MinEntries < 0 {
		config.MinEntries = 0
	}
	if config.MaxEntries < config.MinEntries {
		config.MaxEntries = config.MinEntries
	}
	return config
}

func sharedMemoryTuningStep(limit int, percent float64) int {
	if limit <= 0 {
		return 1
	}
	step := int(math.Ceil(float64(limit) * percent))
	if step < 1 {
		return 1
	}
	return step
}

func clampInt(value int, min int, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
