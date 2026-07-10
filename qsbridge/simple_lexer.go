package qsbridge

import "unicode"

// simpleTokenKind identifies one lexical class in the simple SQL scaffold.
type simpleTokenKind string

const (
	simpleTokenEOF         simpleTokenKind = "eof"
	simpleTokenIdentifier  simpleTokenKind = "identifier"
	simpleTokenKeyword     simpleTokenKind = "keyword"
	simpleTokenNumber      simpleTokenKind = "number"
	simpleTokenString      simpleTokenKind = "string"
	simpleTokenOperator    simpleTokenKind = "operator"
	simpleTokenComma       simpleTokenKind = "comma"
	simpleTokenDot         simpleTokenKind = "dot"
	simpleTokenLeftParen   simpleTokenKind = "left_paren"
	simpleTokenRightParen  simpleTokenKind = "right_paren"
	simpleTokenPlaceholder simpleTokenKind = "placeholder"
	simpleTokenAsterisk    simpleTokenKind = "asterisk"
	simpleTokenSemicolon   simpleTokenKind = "semicolon"
	simpleTokenError       simpleTokenKind = "error"
)

// simpleToken records a lexical token plus its byte span in the SQL text.
type simpleToken struct {
	Kind  simpleTokenKind
	Text  string
	Start int
	End   int
}

type simpleLexer struct {
	text string
	pos  int
}

// lexSimpleSQL tokenizes the simple parser's supported SQL surface.
func lexSimpleSQL(sql string) []simpleToken {
	lexer := simpleLexer{text: sql}
	tokens := make([]simpleToken, 0, len(sql)/4)
	for {
		token := lexer.nextToken()
		tokens = append(tokens, token)
		if token.Kind == simpleTokenEOF || token.Kind == simpleTokenError {
			return tokens
		}
	}
}

// nextToken returns the next token and advances the lexer.
func (lexer *simpleLexer) nextToken() simpleToken {
	lexer.skipWhitespace()
	start := lexer.pos
	if start >= len(lexer.text) {
		return simpleToken{Kind: simpleTokenEOF, Start: start, End: start}
	}
	ch := lexer.text[start]
	switch {
	case isSimpleIdentifierStart(ch):
		return lexer.scanIdentifierOrKeyword()
	case isSimpleDigit(ch):
		return lexer.scanNumber()
	case ch == '\'':
		return lexer.scanString()
	case ch == ',':
		lexer.pos++
		return simpleToken{Kind: simpleTokenComma, Text: ",", Start: start, End: lexer.pos}
	case ch == '.':
		lexer.pos++
		return simpleToken{Kind: simpleTokenDot, Text: ".", Start: start, End: lexer.pos}
	case ch == '(':
		lexer.pos++
		return simpleToken{Kind: simpleTokenLeftParen, Text: "(", Start: start, End: lexer.pos}
	case ch == ')':
		lexer.pos++
		return simpleToken{Kind: simpleTokenRightParen, Text: ")", Start: start, End: lexer.pos}
	case ch == '?':
		lexer.pos++
		return simpleToken{Kind: simpleTokenPlaceholder, Text: "?", Start: start, End: lexer.pos}
	case ch == '*':
		lexer.pos++
		return simpleToken{Kind: simpleTokenAsterisk, Text: "*", Start: start, End: lexer.pos}
	case ch == ';':
		lexer.pos++
		return simpleToken{Kind: simpleTokenSemicolon, Text: ";", Start: start, End: lexer.pos}
	case isSimpleOperatorStart(ch):
		return lexer.scanOperator()
	default:
		lexer.pos++
		return simpleToken{Kind: simpleTokenError, Text: lexer.text[start:lexer.pos], Start: start, End: lexer.pos}
	}
}

// skipWhitespace advances past SQL whitespace.
func (lexer *simpleLexer) skipWhitespace() {
	for lexer.pos < len(lexer.text) && unicode.IsSpace(rune(lexer.text[lexer.pos])) {
		lexer.pos++
	}
}

// scanIdentifierOrKeyword scans an identifier and classifies known SQL keywords.
func (lexer *simpleLexer) scanIdentifierOrKeyword() simpleToken {
	start := lexer.pos
	lexer.pos++
	for lexer.pos < len(lexer.text) && isSimpleIdentifierPart(lexer.text[lexer.pos]) {
		lexer.pos++
	}
	text := lexer.text[start:lexer.pos]
	kind := simpleTokenIdentifier
	if isSimpleKeyword(text) {
		kind = simpleTokenKeyword
	}
	return simpleToken{Kind: kind, Text: text, Start: start, End: lexer.pos}
}

// scanNumber scans an integer or decimal numeric literal.
func (lexer *simpleLexer) scanNumber() simpleToken {
	start := lexer.pos
	for lexer.pos < len(lexer.text) && isSimpleDigit(lexer.text[lexer.pos]) {
		lexer.pos++
	}
	if lexer.pos < len(lexer.text) && lexer.text[lexer.pos] == '.' {
		lexer.pos++
		for lexer.pos < len(lexer.text) && isSimpleDigit(lexer.text[lexer.pos]) {
			lexer.pos++
		}
	}
	return simpleToken{Kind: simpleTokenNumber, Text: lexer.text[start:lexer.pos], Start: start, End: lexer.pos}
}

// scanString scans a single-quoted SQL string with doubled-quote escapes.
func (lexer *simpleLexer) scanString() simpleToken {
	start := lexer.pos
	lexer.pos++
	for lexer.pos < len(lexer.text) {
		if lexer.text[lexer.pos] != '\'' {
			lexer.pos++
			continue
		}
		lexer.pos++
		if lexer.pos < len(lexer.text) && lexer.text[lexer.pos] == '\'' {
			lexer.pos++
			continue
		}
		return simpleToken{Kind: simpleTokenString, Text: lexer.text[start:lexer.pos], Start: start, End: lexer.pos}
	}
	return simpleToken{Kind: simpleTokenError, Text: lexer.text[start:lexer.pos], Start: start, End: lexer.pos}
}

// scanOperator scans comparison and arithmetic operators.
func (lexer *simpleLexer) scanOperator() simpleToken {
	start := lexer.pos
	lexer.pos++
	if lexer.pos < len(lexer.text) {
		pair := lexer.text[start : lexer.pos+1]
		switch pair {
		case ">=", "<=", "<>", "!=":
			lexer.pos++
		}
	}
	return simpleToken{Kind: simpleTokenOperator, Text: lexer.text[start:lexer.pos], Start: start, End: lexer.pos}
}

// isSimpleKeyword reports whether text is a simple SQL keyword.
func isSimpleKeyword(text string) bool {
	switch simpleLowerASCII(text) {
	case "and", "as", "asc", "between", "by", "desc", "false", "from", "group", "having", "in", "insert", "into", "is", "limit", "not", "null", "offset", "or", "order", "select", "true", "values", "where":
		return true
	default:
		return false
	}
}

// isSimpleIdentifierStart reports whether ch can start a simple SQL identifier.
func isSimpleIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

// isSimpleIdentifierPart reports whether ch can continue a simple SQL identifier.
func isSimpleIdentifierPart(ch byte) bool {
	return isSimpleIdentifierStart(ch) || isSimpleDigit(ch)
}

// isSimpleDigit reports whether ch is an ASCII digit.
func isSimpleDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// isSimpleOperatorStart reports whether ch starts a supported operator token.
func isSimpleOperatorStart(ch byte) bool {
	switch ch {
	case '=', '<', '>', '!', '+', '-', '/':
		return true
	default:
		return false
	}
}

// simpleLowerASCII lowercases the ASCII SQL keywords used by the simple lexer.
func simpleLowerASCII(text string) string {
	out := []byte(text)
	for i, ch := range out {
		if ch >= 'A' && ch <= 'Z' {
			out[i] = ch + ('a' - 'A')
		}
	}
	return string(out)
}
