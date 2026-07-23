package core

import (
	"bufio"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/shared"
)

func TestStringLexBSIMapperEncodesTPCHLineitemCommentPrefix(t *testing.T) {
	comment := firstTPCHLineitemComment(t)
	mapper, err := NewStringLexBSIMapper(map[string]string{"length": "8"})
	if err != nil {
		t.Fatalf("NewStringLexBSIMapper returned error: %v", err)
	}
	attr := &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: "l_comment", MappingStrategy: "StringLexBSI", Type: "String"},
		Parent:         &Table{BasicTable: &shared.BasicTable{Name: "lineitem"}},
	}

	value, err := mapper.MapValue(attr, comment, nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}

	if got, want := value.Uint64(), uint64(0x6567756c61722063); got != want {
		t.Fatalf("encoded prefix = %#x, want %#x", got, want)
	}
	encoded, remainder := encodeStringLexBSI(comment, 8)
	if encoded.Cmp(value) != 0 {
		t.Fatalf("helper encoded value = %s, mapper value = %s", encoded.String(), value.String())
	}
	if remainder != "ourts above the" {
		t.Fatalf("remainder = %q, want %q", remainder, "ourts above the")
	}
	if rendered := mapper.Render(attr, value); rendered != "egular c" {
		t.Fatalf("rendered prefix = %q, want %q", rendered, "egular c")
	}
}

func TestStringLexBSIMapperNonPositiveLengthEncodesFullStringInline(t *testing.T) {
	comment := firstTPCHLineitemComment(t)
	mapper, err := NewStringLexBSIMapper(map[string]string{"length": "0"})
	if err != nil {
		t.Fatalf("NewStringLexBSIMapper returned error: %v", err)
	}
	attr := &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: "l_comment", MappingStrategy: "StringLexBSI", Type: "String"},
		Parent:         &Table{BasicTable: &shared.BasicTable{Name: "lineitem"}},
	}

	value, err := mapper.MapValue(attr, comment, nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	want := new(big.Int).SetBytes([]byte(comment))
	if value.Cmp(want) != 0 {
		t.Fatalf("full inline value = %s, want %s", value.String(), want.String())
	}
	_, remainder := encodeStringLexBSI(comment, 0)
	if remainder != "" {
		t.Fatalf("remainder = %q, want empty", remainder)
	}
}

func BenchmarkStringLexBSIEncodeTPCHLineitemComments8(b *testing.B) {
	comments := tpchLineitemComments(b)
	var totalBytes int64
	for _, comment := range comments {
		totalBytes += int64(len(comment))
	}
	b.ReportAllocs()
	b.SetBytes(totalBytes / int64(len(comments)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeStringLexBSI(comments[i%len(comments)], 8)
	}
}

func firstTPCHLineitemComment(t testing.TB) string {
	t.Helper()
	comments := tpchLineitemComments(t)
	if len(comments) == 0 {
		t.Fatalf("no TPCH lineitem comments loaded")
	}
	return comments[0]
}

func tpchLineitemComments(t testing.TB) []string {
	t.Helper()
	path := filepath.Join("..", "tpc-h-benchmark", "local", "data", "sf-0.01", "lineitem.tbl")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open TPCH lineitem fixture %s: %v", path, err)
	}
	defer file.Close()

	var comments []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "|")
		if len(fields) < 16 {
			t.Fatalf("lineitem fixture row has %d fields, want at least 16", len(fields))
		}
		comments = append(comments, fields[15])
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan TPCH lineitem fixture: %v", err)
	}
	return comments
}
