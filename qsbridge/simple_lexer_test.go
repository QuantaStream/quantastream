package qsbridge

import "testing"

func TestLexSimpleSQLTokenizesSelectSurface(t *testing.T) {
	sql := "select o.o_custkey, count(*) as order_count from orders as o where o.o_totalprice >= 101.5 and o.o_name = 'Bob''s' order by count(*) desc limit 1 offset ?;"
	tokens := lexSimpleSQL(sql)

	got := tokenSummaries(tokens)
	want := []string{
		"keyword:select",
		"identifier:o",
		"dot:.",
		"identifier:o_custkey",
		"comma:,",
		"identifier:count",
		"left_paren:(",
		"asterisk:*",
		"right_paren:)",
		"keyword:as",
		"identifier:order_count",
		"keyword:from",
		"identifier:orders",
		"keyword:as",
		"identifier:o",
		"keyword:where",
		"identifier:o",
		"dot:.",
		"identifier:o_totalprice",
		"operator:>=",
		"number:101.5",
		"keyword:and",
		"identifier:o",
		"dot:.",
		"identifier:o_name",
		"operator:=",
		"string:'Bob''s'",
		"keyword:order",
		"keyword:by",
		"identifier:count",
		"left_paren:(",
		"asterisk:*",
		"right_paren:)",
		"keyword:desc",
		"keyword:limit",
		"number:1",
		"keyword:offset",
		"placeholder:?",
		"semicolon:;",
		"eof:",
	}
	if len(got) != len(want) {
		t.Fatalf("token count = %d, want %d\n got %#v\nwant %#v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] = %q, want %q\n got %#v", i, got[i], want[i], got)
		}
	}

	if tokens[0].Start != 0 || tokens[0].End != len("select") {
		t.Fatalf("first token span = %d:%d, want 0:6", tokens[0].Start, tokens[0].End)
	}
	stringToken := tokens[26]
	stringStart := 106
	stringEnd := stringStart + len("'Bob''s'")
	if stringToken.Start != stringStart || stringToken.End != stringEnd {
		t.Fatalf("string token span = %d:%d, want %d:%d in %q", stringToken.Start, stringToken.End, stringStart, stringEnd, sql)
	}
}

func TestLexSimpleSQLTokenizesOperators(t *testing.T) {
	tokens := lexSimpleSQL("a >= 1 and b <= 2 and c <> 3 and d != 4 and e < 5 and f > 6 and g = 7 and h + i - j / k * l")

	got := tokenSummaries(tokens)
	want := []string{
		"identifier:a",
		"operator:>=",
		"number:1",
		"keyword:and",
		"identifier:b",
		"operator:<=",
		"number:2",
		"keyword:and",
		"identifier:c",
		"operator:<>",
		"number:3",
		"keyword:and",
		"identifier:d",
		"operator:!=",
		"number:4",
		"keyword:and",
		"identifier:e",
		"operator:<",
		"number:5",
		"keyword:and",
		"identifier:f",
		"operator:>",
		"number:6",
		"keyword:and",
		"identifier:g",
		"operator:=",
		"number:7",
		"keyword:and",
		"identifier:h",
		"operator:+",
		"identifier:i",
		"operator:-",
		"identifier:j",
		"operator:/",
		"identifier:k",
		"asterisk:*",
		"identifier:l",
		"eof:",
	}
	if len(got) != len(want) {
		t.Fatalf("token count = %d, want %d\n got %#v\nwant %#v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] = %q, want %q\n got %#v", i, got[i], want[i], got)
		}
	}
}

func TestLexSimpleSQLStopsOnUnknownInput(t *testing.T) {
	tokens := lexSimpleSQL("select @bad")
	if got, want := tokens[len(tokens)-1].Kind, simpleTokenError; got != want {
		t.Fatalf("last token kind = %s, want %s; tokens=%#v", got, want, tokens)
	}
	if got, want := tokens[len(tokens)-1].Text, "@"; got != want {
		t.Fatalf("error token text = %q, want %q", got, want)
	}
}

func TestLexSimpleSQLStopsOnUnterminatedString(t *testing.T) {
	tokens := lexSimpleSQL("select 'open")
	if got, want := tokens[len(tokens)-1].Kind, simpleTokenError; got != want {
		t.Fatalf("last token kind = %s, want %s; tokens=%#v", got, want, tokens)
	}
	if got, want := tokens[len(tokens)-1].Text, "'open"; got != want {
		t.Fatalf("error token text = %q, want %q", got, want)
	}
}

func tokenSummaries(tokens []simpleToken) []string {
	summaries := make([]string, 0, len(tokens))
	for _, token := range tokens {
		summaries = append(summaries, string(token.Kind)+":"+token.Text)
	}
	return summaries
}
