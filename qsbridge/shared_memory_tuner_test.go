package qsbridge

import "testing"

func TestSharedMemoryTunerObserveEstablishesBaseline(t *testing.T) {
	tuner := newSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     10,
		MaxEntries:     200,
		TargetHitRatio: 0.80,
		MinimumLookups: 10,
	})

	decision := tuner.Observe(shardedValueCacheStats{MaxEntries: 100})
	if decision.Action != sharedMemoryTuningHold || decision.Reason != "baseline" || decision.NextLimit != 100 {
		t.Fatalf("decision = %#v, want baseline hold", decision)
	}
}

func TestSharedMemoryTunerObserveGrowsBelowTarget(t *testing.T) {
	tuner := newSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     10,
		MaxEntries:     200,
		TargetHitRatio: 0.80,
		MinimumLookups: 10,
		GrowPercent:    0.25,
	})
	tuner.Observe(shardedValueCacheStats{MaxEntries: 100, Hits: 0, Misses: 0})

	decision := tuner.Observe(shardedValueCacheStats{MaxEntries: 100, Hits: 5, Misses: 15})
	if decision.Action != sharedMemoryTuningGrow || decision.Reason != "below_target" || decision.NextLimit != 125 {
		t.Fatalf("decision = %#v, want grow to 125", decision)
	}
	if decision.IntervalHits != 5 || decision.IntervalMisses != 15 || decision.IntervalHitRatio != 0.25 {
		t.Fatalf("decision = %#v, want interval counters", decision)
	}
}

func TestSharedMemoryTunerObserveGrowsOnEvictionPressure(t *testing.T) {
	tuner := newSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     10,
		MaxEntries:     200,
		TargetHitRatio: 0.80,
		MinimumLookups: 10,
	})
	tuner.Observe(shardedValueCacheStats{MaxEntries: 100})

	decision := tuner.Observe(shardedValueCacheStats{MaxEntries: 100, Hits: 6, Misses: 14, Evictions: 3})
	if decision.Action != sharedMemoryTuningGrow || decision.Reason != "eviction_pressure" {
		t.Fatalf("decision = %#v, want eviction-pressure growth", decision)
	}
	if decision.IntervalEvictions != 3 {
		t.Fatalf("decision = %#v, want eviction delta", decision)
	}
}

func TestSharedMemoryTunerObserveShrinksWhenAboveTarget(t *testing.T) {
	tuner := newSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     10,
		MaxEntries:     200,
		TargetHitRatio: 0.80,
		MinimumLookups: 10,
		ShrinkPercent:  0.25,
	})
	tuner.Observe(shardedValueCacheStats{MaxEntries: 100})

	decision := tuner.Observe(shardedValueCacheStats{MaxEntries: 100, Hits: 19, Misses: 1})
	if decision.Action != sharedMemoryTuningShrink || decision.Reason != "above_target" || decision.NextLimit != 75 {
		t.Fatalf("decision = %#v, want shrink to 75", decision)
	}
}

func TestSharedMemoryTunerObserveShrinksWhenGrowthDidNotHelp(t *testing.T) {
	tuner := newSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     10,
		MaxEntries:     200,
		TargetHitRatio: 0.75,
		MinimumLookups: 10,
		ShrinkPercent:  0.10,
	})
	tuner.state = sharedMemoryTunerState{
		initialized:              true,
		previousHits:             100,
		previousMisses:           0,
		previousLimit:            100,
		previousIntervalHitRatio: 0.90,
	}

	decision := tuner.Observe(shardedValueCacheStats{MaxEntries: 120, Hits: 180, Misses: 20})
	if decision.Action != sharedMemoryTuningShrink || decision.Reason != "growth_no_benefit" || decision.NextLimit != 108 {
		t.Fatalf("decision = %#v, want no-benefit shrink", decision)
	}
}

func TestSharedMemoryTunerObserveHoldsForGuardrails(t *testing.T) {
	tests := []struct {
		name   string
		tuner  *sharedMemoryTuner
		stats  shardedValueCacheStats
		reason string
	}{
		{
			name:   "disabled",
			tuner:  newSharedMemoryTuner(sharedMemoryTunerConfig{Enabled: false, MaxEntries: 100}),
			stats:  shardedValueCacheStats{MaxEntries: 100, Hits: 10},
			reason: "disabled",
		},
		{
			name:   "unbounded",
			tuner:  initializedSharedMemoryTuner(sharedMemoryTunerConfig{Enabled: true, MaxEntries: 100}),
			stats:  shardedValueCacheStats{MaxEntries: -1, Hits: 10},
			reason: "unbounded",
		},
		{
			name: "insufficient samples",
			tuner: initializedSharedMemoryTuner(sharedMemoryTunerConfig{
				Enabled:        true,
				MaxEntries:     100,
				MinimumLookups: 10,
			}),
			stats:  shardedValueCacheStats{MaxEntries: 100, Hits: 2, Misses: 1},
			reason: "insufficient_samples",
		},
		{
			name: "counter reset",
			tuner: &sharedMemoryTuner{
				config: normalizeSharedMemoryTunerConfig(sharedMemoryTunerConfig{Enabled: true, MaxEntries: 100}),
				state: sharedMemoryTunerState{
					initialized:    true,
					previousHits:   10,
					previousMisses: 10,
				},
			},
			stats:  shardedValueCacheStats{MaxEntries: 100, Hits: 1, Misses: 1},
			reason: "counter_reset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := tt.tuner.Observe(tt.stats)
			if decision.Action != sharedMemoryTuningHold || decision.Reason != tt.reason {
				t.Fatalf("decision = %#v, want hold reason %s", decision, tt.reason)
			}
		})
	}
}

func TestSharedMemoryTunerObserveClampsAtBounds(t *testing.T) {
	grow := initializedSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     50,
		MaxEntries:     110,
		TargetHitRatio: 0.80,
		MinimumLookups: 10,
		GrowPercent:    0.50,
	})
	growDecision := grow.Observe(shardedValueCacheStats{MaxEntries: 100, Hits: 1, Misses: 9})
	if growDecision.NextLimit != 110 {
		t.Fatalf("grow decision = %#v, want max clamp", growDecision)
	}

	shrink := initializedSharedMemoryTuner(sharedMemoryTunerConfig{
		Enabled:        true,
		MinEntries:     90,
		MaxEntries:     200,
		TargetHitRatio: 0.80,
		MinimumLookups: 10,
		ShrinkPercent:  0.50,
	})
	shrinkDecision := shrink.Observe(shardedValueCacheStats{MaxEntries: 100, Hits: 10, Misses: 0})
	if shrinkDecision.NextLimit != 90 {
		t.Fatalf("shrink decision = %#v, want min clamp", shrinkDecision)
	}
}

func initializedSharedMemoryTuner(config sharedMemoryTunerConfig) *sharedMemoryTuner {
	tuner := newSharedMemoryTuner(config)
	tuner.state.initialized = true
	return tuner
}
