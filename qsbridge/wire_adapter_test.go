package qsbridge

import "testing"

func TestDefaultWireAdapterBoundariesSeparateWireAndPlanningOwnership(t *testing.T) {
	boundaries := DefaultWireAdapterBoundaries()
	packet, ok := wireAdapterBoundaryByConcern(boundaries, ProtocolMySQL, WireAdapterConcernPacketIO)
	if !ok || packet.Owner != WireAdapterOwnerProtocolAdapter || !packet.Permanent || packet.MetadataOnly {
		t.Fatalf("packet boundary = %#v/%v, want permanent MySQL protocol adapter ownership", packet, ok)
	}

	planning, ok := wireAdapterBoundaryByConcern(boundaries, ProtocolUnknown, WireAdapterConcernSQLPlanning)
	if !ok || planning.Owner != WireAdapterOwnerQSBridge || !planning.MetadataOnly || !planning.Permanent {
		t.Fatalf("planning boundary = %#v/%v, want permanent qsbridge metadata ownership", planning, ok)
	}

	result, ok := wireAdapterBoundaryByConcern(boundaries, ProtocolUnknown, WireAdapterConcernResultMetadata)
	if !ok || result.Owner != WireAdapterOwnerQSBridge || !result.MetadataOnly {
		t.Fatalf("result boundary = %#v/%v, want qsbridge result metadata ownership", result, ok)
	}

	serialization, ok := wireAdapterBoundaryByConcern(boundaries, ProtocolMySQL, WireAdapterConcernResultSerialization)
	if !ok || serialization.Owner != WireAdapterOwnerProtocolAdapter || serialization.MetadataOnly {
		t.Fatalf("serialization boundary = %#v/%v, want MySQL adapter result serialization ownership", serialization, ok)
	}
}

func TestWireAdapterBoundariesForProtocolIncludesCommonAndProtocolRows(t *testing.T) {
	mysql := WireAdapterBoundariesForProtocol(ProtocolMySQL)
	if !wireAdapterBoundariesContain(mysql, ProtocolMySQL, WireAdapterConcernPacketIO) {
		t.Fatalf("mysql boundaries = %#v, want MySQL packet IO", mysql)
	}
	if !wireAdapterBoundariesContain(mysql, ProtocolUnknown, WireAdapterConcernSQLPlanning) {
		t.Fatalf("mysql boundaries = %#v, want common SQL planning", mysql)
	}
	if wireAdapterBoundariesContain(mysql, ProtocolGRPC, WireAdapterConcernPacketIO) {
		t.Fatalf("mysql boundaries = %#v, did not expect gRPC packet IO", mysql)
	}

	grpc := WireAdapterBoundariesForProtocol(ProtocolGRPC)
	if !wireAdapterBoundariesContain(grpc, ProtocolGRPC, WireAdapterConcernPacketIO) {
		t.Fatalf("grpc boundaries = %#v, want gRPC transport boundary", grpc)
	}
	if !wireAdapterBoundariesContain(grpc, ProtocolUnknown, WireAdapterConcernHandoff) {
		t.Fatalf("grpc boundaries = %#v, want common handoff boundary", grpc)
	}
}

func TestDefaultWireAdapterBoundariesReturnCopies(t *testing.T) {
	first := DefaultWireAdapterBoundaries()
	first[0].Detail = "mutated"

	second := DefaultWireAdapterBoundaries()
	if second[0].Detail == "mutated" {
		t.Fatalf("wire adapter boundaries leaked mutable state: %#v", second[0])
	}
}

func wireAdapterBoundaryByConcern(boundaries []WireAdapterBoundary, protocol ProtocolKind, concern WireAdapterConcern) (WireAdapterBoundary, bool) {
	for _, boundary := range boundaries {
		if boundary.Protocol == protocol && boundary.Concern == concern {
			return boundary, true
		}
	}
	return WireAdapterBoundary{}, false
}

func wireAdapterBoundariesContain(boundaries []WireAdapterBoundary, protocol ProtocolKind, concern WireAdapterConcern) bool {
	_, ok := wireAdapterBoundaryByConcern(boundaries, protocol, concern)
	return ok
}
