package qsbridge

import "testing"

func TestDefaultAdapterContractsSeparateMetadataAdapterAndRuntimeOwnership(t *testing.T) {
	contracts := DefaultAdapterContracts()
	mysqlPlanning, ok := adapterContractBySurfaceConcern(contracts, AdapterSurfaceMySQLServer, AdapterContractStatementPlanning)
	if !ok || mysqlPlanning.Owner != WireAdapterOwnerQSBridge || mysqlPlanning.Status != CompatibilityStatusMetadataOnly {
		t.Fatalf("mysql planning contract = %#v/%v, want qsbridge metadata ownership", mysqlPlanning, ok)
	}

	mysqlSerialization, ok := adapterContractBySurfaceConcern(contracts, AdapterSurfaceMySQLServer, AdapterContractResultSerialization)
	if !ok || !mysqlSerialization.AdapterOwned || mysqlSerialization.Owner != WireAdapterOwnerProtocolAdapter {
		t.Fatalf("mysql serialization contract = %#v/%v, want protocol adapter ownership", mysqlSerialization, ok)
	}

	embeddedDispatch, ok := adapterContractBySurfaceConcern(contracts, AdapterSurfaceEmbedded, AdapterContractExecutionDispatch)
	if !ok || !embeddedDispatch.RuntimeOwned || embeddedDispatch.Owner != WireAdapterOwnerExecutor {
		t.Fatalf("embedded dispatch contract = %#v/%v, want runtime executor ownership", embeddedDispatch, ok)
	}

	topology, ok := adapterContractBySurfaceConcern(contracts, AdapterSurfaceInternalExecution, AdapterContractTopology)
	if !ok || topology.Status != CompatibilityStatusDeferred || !topology.RuntimeOwned {
		t.Fatalf("internal topology contract = %#v/%v, want deferred runtime-owned topology", topology, ok)
	}
}

func TestAdapterContractsForSurfaceFiltersRows(t *testing.T) {
	mysql := AdapterContractsForSurface(AdapterSurfaceMySQLServer)
	if !adapterContractsContain(mysql, AdapterSurfaceMySQLServer, AdapterContractProtocolDecode) ||
		!adapterContractsContain(mysql, AdapterSurfaceMySQLServer, AdapterContractPreparedExecution) {
		t.Fatalf("mysql contracts = %#v, want protocol decode and prepared execution", mysql)
	}
	if adapterContractsContain(mysql, AdapterSurfaceGRPCAPI, AdapterContractInspection) {
		t.Fatalf("mysql contracts = %#v, did not expect gRPC inspection", mysql)
	}

	grpc := AdapterContractsForSurface(AdapterSurfaceGRPCAPI)
	if !adapterContractsContain(grpc, AdapterSurfaceGRPCAPI, AdapterContractInspection) {
		t.Fatalf("grpc contracts = %#v, want inspection/control-plane contract", grpc)
	}
}

func TestDefaultAdapterContractsReturnCopies(t *testing.T) {
	first := DefaultAdapterContracts()
	first[0].Detail = "mutated"

	second := DefaultAdapterContracts()
	if second[0].Detail == "mutated" {
		t.Fatalf("adapter contracts leaked mutable state: %#v", second[0])
	}
}

func adapterContractBySurfaceConcern(contracts []AdapterContract, surface AdapterSurfaceKind, concern AdapterContractConcern) (AdapterContract, bool) {
	for _, contract := range contracts {
		if contract.Surface == surface && contract.Concern == concern {
			return contract, true
		}
	}
	return AdapterContract{}, false
}

func adapterContractsContain(contracts []AdapterContract, surface AdapterSurfaceKind, concern AdapterContractConcern) bool {
	_, ok := adapterContractBySurfaceConcern(contracts, surface, concern)
	return ok
}
