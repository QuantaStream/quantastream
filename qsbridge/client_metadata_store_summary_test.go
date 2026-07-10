package qsbridge

import "testing"

func TestPlanningServiceListClientMetadataStoreSummaryReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientMetadataStoreSummary(clientStatementConnection(), "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported metadata-store summary", exchange)
	}
	if len(exchange.Profiles) != len(DefaultMetadataStoreProfiles()) {
		t.Fatalf("profiles = %#v, want default metadata store profiles", exchange.Profiles)
	}
	if len(exchange.ResultSchema.Columns) != 12 || exchange.ResultSchema.Columns[0].Name != "Name" {
		t.Fatalf("schema = %#v, want metadata-store columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Profiles)) {
		t.Fatalf("result = %#v, want one row per metadata store profile", exchange.Result)
	}
	catalog, ok := metadataStoreSummaryByDomain(exchange.Profiles, MetadataStoreDomainCatalog)
	if !ok || catalog.Backend != MetadataStoreBackendConsul || !catalog.CacheRequired ||
		catalog.CacheScope != MetadataCacheScopeProcess || catalog.Invalidation != MetadataInvalidationWatch {
		t.Fatalf("catalog profile = %#v/%v, want cached Consul metadata", catalog, ok)
	}
	dictionary, ok := metadataStoreSummaryByName(exchange.Profiles, "string_enum_dictionary")
	if !ok || dictionary.Backend != MetadataStoreBackendKVStore || !dictionary.CacheRequired ||
		dictionary.CacheScope != MetadataCacheScopeProcess || dictionary.Invalidation != MetadataInvalidationVersioned ||
		!dictionary.Mutable || !dictionary.Distributed || !dictionary.RuntimeOwned || !dictionary.NodeLocalCopy {
		t.Fatalf("dictionary profile = %#v/%v, want cached KVStore dictionary with node-local copies", dictionary, ok)
	}
	roles, ok := metadataStoreSummaryByName(exchange.Profiles, "user_role_metadata")
	if !ok || roles.Backend != MetadataStoreBackendConsul || !roles.CacheRequired ||
		roles.CacheScope != MetadataCacheScopeProcess || roles.Invalidation != MetadataInvalidationWatch || !roles.Distributed {
		t.Fatalf("role metadata profile = %#v/%v, want cached distributed metadata", roles, ok)
	}
}

func TestPlanningServiceListClientMetadataStoreSummaryFiltersByPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientMetadataStoreSummary(clientStatementConnection(), "dictionary")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered metadata", exchange)
	}
	if len(exchange.Profiles) != 1 || exchange.Profiles[0].Domain != MetadataStoreDomainDictionary {
		t.Fatalf("profiles = %#v, want dictionary profile", exchange.Profiles)
	}
}

func TestPlanningServiceListClientMetadataStoreSummaryReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhaseBind, "blocked")}

	exchange := service.ListClientMetadataStoreSummary(connection, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocked connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 12 {
		t.Fatalf("result/schema = %#v/%#v, want failed metadata-store envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientMetadataStoreSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientMetadataStoreSummary(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Profiles[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientMetadataStoreSummary(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Profiles[0].Name == "mutated" {
		t.Fatalf("profiles leaked mutation: %#v", again.Profiles[0])
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks[0].Rows[0])
	}
}

func metadataStoreSummaryByDomain(profiles []MetadataStoreProfile, domain MetadataStoreDomain) (MetadataStoreProfile, bool) {
	for _, profile := range profiles {
		if profile.Domain == domain {
			return profile, true
		}
	}
	return MetadataStoreProfile{}, false
}

func metadataStoreSummaryByName(profiles []MetadataStoreProfile, name string) (MetadataStoreProfile, bool) {
	for _, profile := range profiles {
		if profile.Name == name {
			return profile, true
		}
	}
	return MetadataStoreProfile{}, false
}
