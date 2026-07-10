package qsbridge

import "testing"

func TestDefaultAdapterSurfacesNamePublicAndInternalLanes(t *testing.T) {
	surfaces := DefaultAdapterSurfaces()
	mysql, ok := adapterSurfaceByKind(surfaces, AdapterSurfaceMySQLServer)
	if !ok || !mysql.ClientFacing || mysql.Protocol != ProtocolMySQL || mysql.Transport != TransportKindMySQLWire {
		t.Fatalf("mysql surface = %#v/%v, want client-facing MySQL wire surface", mysql, ok)
	}
	if mysql.ControlPlane || mysql.Internal || mysql.Embedded {
		t.Fatalf("mysql surface = %#v, should not be control, internal, or embedded", mysql)
	}

	grpc, ok := adapterSurfaceByKind(surfaces, AdapterSurfaceGRPCAPI)
	if !ok || !grpc.ClientFacing || !grpc.ControlPlane || grpc.Protocol != ProtocolGRPC {
		t.Fatalf("grpc surface = %#v/%v, want client-facing control-plane API", grpc, ok)
	}

	embedded, ok := adapterSurfaceByKind(surfaces, AdapterSurfaceEmbedded)
	if !ok || !embedded.Embedded || embedded.Transport != TransportKindInProcess || embedded.ClientFacing {
		t.Fatalf("embedded surface = %#v/%v, want non-client in-process QIAB surface", embedded, ok)
	}

	internal, ok := adapterSurfaceByKind(surfaces, AdapterSurfaceInternalExecution)
	if !ok || !internal.Internal || internal.Transport != TransportKindQuantaInternal || internal.ClientFacing {
		t.Fatalf("internal surface = %#v/%v, want non-client internal execution surface", internal, ok)
	}
}

func TestAdapterSurfacesForAudienceFiltersRows(t *testing.T) {
	control := AdapterSurfacesForAudience(AdapterSurfaceAudienceControlPlane)
	if len(control) != 1 || control[0].Kind != AdapterSurfaceGRPCAPI {
		t.Fatalf("control surfaces = %#v, want gRPC API only", control)
	}

	sqlClients := AdapterSurfacesForAudience(AdapterSurfaceAudienceSQLClient)
	if len(sqlClients) != 1 || sqlClients[0].Kind != AdapterSurfaceMySQLServer {
		t.Fatalf("sql client surfaces = %#v, want MySQL server only", sqlClients)
	}
}

func TestDefaultAdapterSurfacesReturnCopies(t *testing.T) {
	first := DefaultAdapterSurfaces()
	first[0].Detail = "mutated"

	second := DefaultAdapterSurfaces()
	if second[0].Detail == "mutated" {
		t.Fatalf("adapter surfaces leaked mutable state: %#v", second[0])
	}
}

func adapterSurfaceByKind(surfaces []AdapterSurface, kind AdapterSurfaceKind) (AdapterSurface, bool) {
	for _, surface := range surfaces {
		if surface.Kind == kind {
			return surface, true
		}
	}
	return AdapterSurface{}, false
}
