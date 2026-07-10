package qsbridge

import "sort"

// ClientAuthenticationMethod describes one adapter-visible authentication option.
type ClientAuthenticationMethod struct {
	Method           AuthenticationMethod
	Plugin           string
	Description      string
	Default          bool
	Enabled          bool
	PasswordExchange bool
	TokenExchange    bool
	ExternalIdentity bool
}

// ClientAuthenticationMethodSummaryRow describes aggregate authentication method metadata.
type ClientAuthenticationMethodSummaryRow struct {
	MethodCount           int
	DefaultCount          int
	EnabledCount          int
	DisabledCount         int
	PasswordExchangeCount int
	TokenExchangeCount    int
	ExternalIdentityCount int
	MySQLPasswordCount    int
	JWTCount              int
	OAuthCount            int
}

// ClientAuthenticationMethodsExchange is adapter-facing authentication metadata.
type ClientAuthenticationMethodsExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Methods      []ClientAuthenticationMethod
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// DefaultClientAuthenticationMethods returns the built-in method inventory adapters can expose.
func DefaultClientAuthenticationMethods() []ClientAuthenticationMethod {
	return []ClientAuthenticationMethod{
		{
			Method:           AuthenticationMethodMySQLPassword,
			Plugin:           string(AuthenticationPluginCachingSHA2Password),
			Description:      "mysql-compatible SHA-2 password flow",
			Default:          true,
			Enabled:          true,
			PasswordExchange: true,
		},
		{
			Method:           AuthenticationMethodMySQLPassword,
			Plugin:           string(AuthenticationPluginMySQLNativePassword),
			Description:      "legacy mysql-compatible password flow",
			Enabled:          true,
			PasswordExchange: true,
		},
		{
			Method:           AuthenticationMethodMySQLPassword,
			Plugin:           string(AuthenticationPluginMySQLClearPassword),
			Description:      "cleartext password flow for secure transports",
			Enabled:          false,
			PasswordExchange: true,
		},
		{
			Method:           AuthenticationMethodJWT,
			Plugin:           string(AuthenticationPluginBearerJWT),
			Description:      "bearer token or JWT flow",
			Enabled:          false,
			TokenExchange:    true,
			ExternalIdentity: true,
		},
		{
			Method:           AuthenticationMethodOAuth,
			Plugin:           string(AuthenticationPluginOpenIDConnect),
			Description:      "OAuth or OpenID Connect flow",
			Enabled:          false,
			TokenExchange:    true,
			ExternalIdentity: true,
		},
	}
}

// ListClientAuthenticationMethods returns adapter-supplied authentication method metadata.
func (s PlanningService) ListClientAuthenticationMethods(connection ConnectionContext, methods []ClientAuthenticationMethod, pattern string) ClientAuthenticationMethodsExchange {
	_ = s
	exchange := ClientAuthenticationMethodsExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Methods = filterClientAuthenticationMethods(methods, pattern)
	}
	exchange.Result = exchange.authenticationMethodsResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether authentication method metadata can be returned.
func (e ClientAuthenticationMethodsExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts authentication method diagnostics into protocol-facing errors.
func (e ClientAuthenticationMethodsExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking authentication method error, if any.
func (e ClientAuthenticationMethodsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAuthenticationMethodsExchange) authenticationMethodsResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     authenticationMethodsResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.authenticationMethodRows(),
		Final: true,
	})
}

func authenticationMethodsResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Method", Type: DataTypeString},
		{Name: "Plugin", Type: DataTypeString, Nullable: true},
		{Name: "Description", Type: DataTypeString, Nullable: true},
		{Name: "Default", Type: DataTypeBool},
		{Name: "Enabled", Type: DataTypeBool},
		{Name: "Password_exchange", Type: DataTypeBool},
		{Name: "Token_exchange", Type: DataTypeBool},
		{Name: "External_identity", Type: DataTypeBool},
	}
}

func (e ClientAuthenticationMethodsExchange) authenticationMethodRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Methods))
	for _, method := range e.Methods {
		rows = append(rows, ResultRow{
			metadataStringCell(string(method.Method)),
			metadataStringCell(method.Plugin),
			metadataStringCell(method.Description),
			metadataBoolCell(method.Default),
			metadataBoolCell(method.Enabled),
			metadataBoolCell(method.PasswordExchange),
			metadataBoolCell(method.TokenExchange),
			metadataBoolCell(method.ExternalIdentity),
		})
	}
	return rows
}

func filterClientAuthenticationMethods(methods []ClientAuthenticationMethod, pattern string) []ClientAuthenticationMethod {
	cloned := cloneClientAuthenticationMethods(methods)
	sort.Slice(cloned, func(i, j int) bool {
		if cloned[i].Method != cloned[j].Method {
			return cloned[i].Method < cloned[j].Method
		}
		return cloned[i].Plugin < cloned[j].Plugin
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientAuthenticationMethod, 0, len(cloned))
	for _, method := range cloned {
		if catalogFieldPatternMatch(pattern, string(method.Method)) ||
			catalogFieldPatternMatch(pattern, method.Plugin) ||
			catalogFieldPatternMatch(pattern, method.Description) {
			filtered = append(filtered, method)
		}
	}
	return filtered
}

func summarizeClientAuthenticationMethods(methods []ClientAuthenticationMethod) ClientAuthenticationMethodSummaryRow {
	summary := ClientAuthenticationMethodSummaryRow{MethodCount: len(methods)}
	for _, method := range methods {
		if method.Default {
			summary.DefaultCount++
		}
		if method.Enabled {
			summary.EnabledCount++
		} else {
			summary.DisabledCount++
		}
		if method.PasswordExchange {
			summary.PasswordExchangeCount++
		}
		if method.TokenExchange {
			summary.TokenExchangeCount++
		}
		if method.ExternalIdentity {
			summary.ExternalIdentityCount++
		}
		switch method.Method {
		case AuthenticationMethodMySQLPassword:
			summary.MySQLPasswordCount++
		case AuthenticationMethodJWT:
			summary.JWTCount++
		case AuthenticationMethodOAuth:
			summary.OAuthCount++
		}
	}
	return summary
}

func cloneClientAuthenticationMethods(methods []ClientAuthenticationMethod) []ClientAuthenticationMethod {
	if len(methods) == 0 {
		return nil
	}
	return append([]ClientAuthenticationMethod(nil), methods...)
}
