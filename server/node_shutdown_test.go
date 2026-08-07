package server

import "testing"

type countingNodeService struct {
	shutdowns int
}

func (s *countingNodeService) Init() error {
	return nil
}

func (s *countingNodeService) JoinCluster() {
}

func (s *countingNodeService) Shutdown() {
	s.shutdowns++
}

func TestNodeShutdownServicesIsIdempotent(t *testing.T) {
	service := &countingNodeService{}
	node := &Node{
		localServices: map[string]NodeService{
			"counting": service,
		},
	}

	node.ShutdownServices()
	node.ShutdownServices()

	if service.shutdowns != 1 {
		t.Fatalf("shutdowns = %d, want 1", service.shutdowns)
	}
}
