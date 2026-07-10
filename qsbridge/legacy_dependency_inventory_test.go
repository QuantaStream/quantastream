package qsbridge

import "testing"

func TestLegacyDependencyInventoryClassifiesKnownRetirementSubjects(t *testing.T) {
	tests := []struct {
		subject     string
		disposition LegacyDependencyDisposition
		owner       string
	}{
		{subject: "github.com/QuantaStream/quantastream/core", disposition: LegacyDependencyPreserveBehindInterface, owner: "Guy"},
		{subject: "github.com/QuantaStream/quantastream/source", disposition: LegacyDependencyMoveToCompat, owner: "Codex"},
		{subject: "github.com/QuantaStream/quantastream/shared", disposition: LegacyDependencyPreserveBehindInterface, owner: "Guy"},
		{subject: "github.com/QuantaStream/quantastream/grpc", disposition: LegacyDependencyMoveToCompat, owner: "Guy"},
		{subject: "core/table.go", disposition: LegacyDependencyPreserveBehindInterface, owner: "Guy"},
		{subject: "core/session.go", disposition: LegacyDependencyPreserveBehindInterface, owner: "Guy"},
		{subject: "core/session_pool.go", disposition: LegacyDependencyPreserveBehindInterface, owner: "Guy"},
		{subject: "core.Projector", disposition: LegacyDependencyMoveToCompat, owner: "Codex"},
		{subject: "source/quanta_join.go", disposition: LegacyDependencyDeleteAfterProxyRetirement, owner: "Codex"},
		{subject: "qsruntime/preflight_helper_execution.go", disposition: LegacyDependencyMoveToCompat, owner: "Codex"},
	}
	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			item, ok := LegacyDependencyInventoryForSubject(tt.subject)
			if !ok {
				t.Fatalf("LegacyDependencyInventoryForSubject(%q) not found", tt.subject)
			}
			if item.Disposition != tt.disposition {
				t.Fatalf("disposition = %q, want %q", item.Disposition, tt.disposition)
			}
			if item.Owner != tt.owner {
				t.Fatalf("owner = %q, want %q", item.Owner, tt.owner)
			}
			if item.Kind == "" || item.Reason == "" {
				t.Fatalf("item = %#v, want kind and reason", item)
			}
		})
	}
}

func TestLegacyDependencyInventoryNormalizesPathSeparators(t *testing.T) {
	item, ok := LegacyDependencyInventoryForSubject(`core\session.go`)
	if !ok {
		t.Fatal("windows-style session path not found")
	}
	if item.Subject != "core/session.go" {
		t.Fatalf("subject = %q, want core/session.go", item.Subject)
	}
}

func TestLegacyDependencyInventoryByDispositionReturnsCopies(t *testing.T) {
	items := LegacyDependencyInventoryByDisposition(LegacyDependencyPreserveBehindInterface)
	if len(items) < 3 {
		t.Fatalf("preserve items = %#v, want session/table research items", items)
	}
	items[0].Subject = "mutated"

	again := LegacyDependencyInventoryByDisposition(LegacyDependencyPreserveBehindInterface)
	if again[0].Subject == "mutated" {
		t.Fatal("LegacyDependencyInventoryByDisposition returned aliased items")
	}
}

func TestLegacyDependencyInventoryResearchSpikeOwnsSessionAndTableItems(t *testing.T) {
	for _, subject := range []string{"core/table.go", "core/session.go", "core/session_pool.go"} {
		item, ok := LegacyDependencyInventoryForSubject(subject)
		if !ok {
			t.Fatalf("%s not found", subject)
		}
		if !item.RequiresProxyRetirementResearch() {
			t.Fatalf("%s item = %#v, want research spike ownership", subject, item)
		}
	}
}

func TestDefaultLegacyDependencyInventoryReturnsCopies(t *testing.T) {
	first := DefaultLegacyDependencyInventory()
	if len(first) == 0 {
		t.Fatal("DefaultLegacyDependencyInventory returned no items")
	}
	first[0].Subject = "mutated"

	second := DefaultLegacyDependencyInventory()
	if second[0].Subject == "mutated" {
		t.Fatal("DefaultLegacyDependencyInventory returned aliased items")
	}
}
