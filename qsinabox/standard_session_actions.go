package qsinabox

import (
	"context"
	"fmt"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/golang/protobuf/ptypes/empty"
)

// StandardSessionActionHandler applies SQL session actions that have concrete
// local storage meaning in inabox-standard.
type StandardSessionActionHandler struct {
	Config  StandardConfig
	Backend StandardLocalBackend
}

// HandleNativeProxySessionActions maps commit_transaction to a durable local
// bitmap commit plus authority artifact manifest refresh.
func (h StandardSessionActionHandler) HandleNativeProxySessionActions(ctx context.Context, actions []qsbridge.SessionAction) qsbridge.DiagnosticSet {
	var diagnostics qsbridge.DiagnosticSet
	for _, action := range actions {
		switch action.Kind {
		case qsbridge.SessionActionCommitTransaction:
			diagnostics = append(diagnostics, h.commit(ctx)...)
		}
	}
	return diagnostics
}

func (h StandardSessionActionHandler) commit(ctx context.Context) qsbridge.DiagnosticSet {
	if h.Backend.Services.BitmapIndex == nil {
		return standardSessionActionDiagnostics("inabox-standard commit requested but local bitmap index service is not mounted")
	}
	if err := core.RejectLocalStorageMutationIfQuiesced(h.Config.WithDefaults().DataDir, "commit"); err != nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, err.Error()),
		}
	}
	if _, err := h.Backend.Services.BitmapIndex.Commit(ctx, &empty.Empty{}); err != nil {
		return standardSessionActionDiagnostics(fmt.Sprintf("inabox-standard commit failed: %v", err))
	}
	if _, err := RefreshStandardBSIPrimaryKeyAuthorityManifestArtifacts(h.Config, "standard-sql-commit"); err != nil {
		return standardSessionActionDiagnostics(fmt.Sprintf("inabox-standard BSI primary-key authority manifest refresh failed: %v", err))
	}
	return nil
}

func standardSessionActionDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, message),
	}
}
