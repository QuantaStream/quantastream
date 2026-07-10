package qsbridge

import "strings"

// ClientSchemaSelectionOptions control adapter handling for current-schema changes.
type ClientSchemaSelectionOptions struct {
	ApplySession bool
	Catalog      CatalogMetadata
}

// ClientSchemaSelectionExchange is client-facing metadata for selecting a schema.
type ClientSchemaSelectionExchange struct {
	Connection  ConnectionContext
	Schema      string
	Session     ClientSessionActionExchange
	Response    ProtocolStatementResponse
	Diagnostics DiagnosticSet
}

// PrepareClientUseSchema previews a MySQL-compatible current database change.
func (s PlanningService) PrepareClientUseSchema(connection ConnectionContext, registry SessionRegistry, schema string, options ClientSchemaSelectionOptions) ClientSchemaSelectionExchange {
	exchange := ClientSchemaSelectionExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "schema selection requires a schema name"),
		})
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateClientSchemaSelection(options.Catalog, schema))
	if exchange.Diagnostics.BlocksNative() {
		return exchange
	}

	action := SessionAction{Kind: SessionActionUseSchema, Value: schema}
	statement := StatementResult{
		Status:         "Database changed",
		SessionActions: []SessionAction{action},
	}
	response := statement.ProtocolStatementResponse(connection.Protocol)
	exchange.Response = cloneProtocolStatementResponse(response)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, response.Diagnostics)
	if exchange.Diagnostics.BlocksNative() {
		return exchange
	}

	session := s.PrepareClientSessionActions(connection, registry, []SessionAction{action}, ClientSessionActionOptions{Apply: options.ApplySession})
	exchange.Session = session
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, session.Diagnostics)
	return exchange
}

// Supported reports whether schema selection metadata can proceed.
func (e ClientSchemaSelectionExchange) Supported() bool {
	return e.Connection.Supported() && e.Schema != "" && e.Response.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts schema selection diagnostics into protocol-facing errors.
func (e ClientSchemaSelectionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking schema selection error, if any.
func (e ClientSchemaSelectionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func validateClientSchemaSelection(catalog CatalogMetadata, schema string) DiagnosticSet {
	if catalog == nil {
		return nil
	}
	schemas, diagnostics := catalog.ListSchemas()
	if diagnostics.BlocksNative() {
		return diagnostics
	}
	for _, candidate := range schemas {
		if strings.EqualFold(candidate.Name, schema) {
			return nil
		}
	}
	return DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogSchemaNotFound, PhaseBind, "schema not found: "+schema),
	}
}
