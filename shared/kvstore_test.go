package shared

import (
	"reflect"
	"testing"
)

func TestStringEnumReplicaNodesSkipsPrimary(t *testing.T) {
	tests := []struct {
		name    string
		indices []int
		want    []int
	}{
		{name: "none", indices: nil, want: nil},
		{name: "single primary", indices: []int{2}, want: nil},
		{name: "replicas", indices: []int{2, 0, 1}, want: []int{0, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringEnumReplicaNodes(tt.indices); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("stringEnumReplicaNodes(%v) = %v, want %v", tt.indices, got, tt.want)
			}
		})
	}
}
