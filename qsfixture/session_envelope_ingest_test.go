package qsfixture

import (
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

func TestIngestEnvelopesWithSessionRequiresSession(t *testing.T) {
	_, diagnostics, err := IngestEnvelopesWithSession(SessionEnvelopeIngestRequest{})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if err == nil || !strings.Contains(err.Error(), "session is required") {
		t.Fatalf("error = %v, want session required", err)
	}
}

func TestIngestEnvelopesWithSessionAcceptsEmptyBatch(t *testing.T) {
	result, diagnostics, err := IngestEnvelopesWithSession(SessionEnvelopeIngestRequest{
		Session: &core.Session{},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if err != nil {
		t.Fatalf("IngestEnvelopesWithSession() error = %v", err)
	}
	if len(result.Routes) != 0 || len(result.PutRows) != 0 || result.Profile.RecordCount != 0 {
		t.Fatalf("result = %+v, want empty ingest result", result)
	}
}
