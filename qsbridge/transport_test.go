package qsbridge

import "testing"

func TestDefaultTransportBoundariesSeparateClientInternalAndInProcess(t *testing.T) {
	boundaries := DefaultTransportBoundaries()
	mysql, ok := transportBoundaryByKind(boundaries, TransportKindMySQLWire)
	if !ok || mysql.Role != TransportRoleClientProtocol || mysql.Protocol != ProtocolMySQL || !mysql.Networked {
		t.Fatalf("mysql boundary = %#v/%v, want networked client protocol", mysql, ok)
	}
	if !mysql.PortIndependent || mysql.Owner != WireAdapterOwnerProtocolAdapter {
		t.Fatalf("mysql boundary = %#v, want adapter-owned port-independent metadata", mysql)
	}

	internal, ok := transportBoundaryByKind(boundaries, TransportKindQuantaInternal)
	if !ok || internal.Role != TransportRoleInternalCluster || internal.Placement != ExecutionPlacementNode {
		t.Fatalf("internal boundary = %#v/%v, want internal node placement", internal, ok)
	}
	if internal.Protocol != ProtocolUnknown || internal.Owner != WireAdapterOwnerExecutor {
		t.Fatalf("internal boundary = %#v, want non-client executor ownership", internal)
	}

	embedded, ok := transportBoundaryByKind(boundaries, TransportKindInProcess)
	if !ok || embedded.Role != TransportRoleInProcess || embedded.Networked {
		t.Fatalf("embedded boundary = %#v/%v, want non-networked in-process placement", embedded, ok)
	}
}

func TestTransportBoundariesForRoleFiltersRows(t *testing.T) {
	client := TransportBoundariesForRole(TransportRoleClientProtocol)
	if !transportBoundariesContain(client, TransportKindMySQLWire) ||
		!transportBoundariesContain(client, TransportKindGRPCAPI) {
		t.Fatalf("client boundaries = %#v, want MySQL and gRPC client protocol rows", client)
	}
	if transportBoundariesContain(client, TransportKindQuantaInternal) {
		t.Fatalf("client boundaries = %#v, did not expect internal cluster transport", client)
	}

	internal := TransportBoundariesForRole(TransportRoleInternalCluster)
	if len(internal) != 1 || internal[0].Kind != TransportKindQuantaInternal {
		t.Fatalf("internal boundaries = %#v, want only internal cluster transport", internal)
	}
}

func TestDefaultTransportBoundariesReturnCopies(t *testing.T) {
	first := DefaultTransportBoundaries()
	first[0].Detail = "mutated"

	second := DefaultTransportBoundaries()
	if second[0].Detail == "mutated" {
		t.Fatalf("transport boundaries leaked mutable state: %#v", second[0])
	}
}

func transportBoundaryByKind(boundaries []TransportBoundary, kind TransportKind) (TransportBoundary, bool) {
	for _, boundary := range boundaries {
		if boundary.Kind == kind {
			return boundary, true
		}
	}
	return TransportBoundary{}, false
}

func transportBoundariesContain(boundaries []TransportBoundary, kind TransportKind) bool {
	_, ok := transportBoundaryByKind(boundaries, kind)
	return ok
}
