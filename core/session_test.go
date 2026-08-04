package core

import (
	"testing"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FIXME: make this work or delete. It never finishes. (nobody home at port 4000)
func xTestCreateSession(t *testing.T) {

	conn := shared.NewDefaultConnection("xTestCreateSession")
	conn.ServicePort = 0
	errx := conn.Connect(nil)
	assert.Nil(t, errx)
	tableCache := NewTableCacheStruct()
	c, err := OpenSession(tableCache, "./testdata", "cities", false, conn)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.TableBuffers)
	assert.Equal(t, len(c.TableBuffers), 1)
	assert.NotNil(t, c.TableBuffers["cities"])
}

// func TestCreateRecursiveSession(t *testing.T) {
// 	os.RemoveAll("./testdata/metadata/user")
// 	os.RemoveAll("./testdata/metadata/events")
// 	os.RemoveAll("./testdata/metadata/ab_test")
// 	os.RemoveAll("./testdata/metadata/dss_id")
// 	os.RemoveAll("./testdata/metadata/guest_id")
// 	os.RemoveAll("./testdata/metadata/anonymous_id")
// 	os.RemoveAll("./testdata/metadata/session_id")
// 	os.RemoveAll("./testdata/metadata/subscription_id")
// 	c, err := OpenSession("./testdata", "./testdata/metadata", "user", true, 0, 0, nil)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, c)
// 	assert.Equal(t, len(c.TableBuffers), 6)
// }

func TestNormalizePutRowSourceCachesMapRow(t *testing.T) {
	session := &Session{
		TableBuffers: map[string]*TableBuffer{
			"customers": {},
		},
	}
	row := map[string]interface{}{
		"id":   "1",
		"name": "Alice",
	}
	req := putRowRequest{
		tableName: "customers",
		row:       row,
	}

	err := session.normalizePutRowSource(&req)

	require.NoError(t, err)
	assert.Equal(t, "/", req.pqTablePath)
	assert.Equal(t, row, session.TableBuffers["customers"].rowCache)
}

func TestNormalizePutRowSourceRejectsMissingTableBuffer(t *testing.T) {
	session := &Session{TableBuffers: map[string]*TableBuffer{}}
	req := putRowRequest{
		tableName: "customers",
		row:       map[string]interface{}{"id": "1"},
	}

	err := session.normalizePutRowSource(&req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot locate buffer for table customers")
}

func TestPutRowWithOptionsRejectsUnsupportedRowType(t *testing.T) {
	session := &Session{TableBuffers: map[string]*TableBuffer{}}

	result, err := session.PutRowWithOptions("customers", struct{}{}, 0, false, false, PutRowOptions{
		EventID: "event-1",
		Source:  "unit-test",
	})

	require.Error(t, err)
	assert.Equal(t, PutRowResult{}, result)
	assert.Contains(t, err.Error(), "cannot process row type")
}

func TestMapAlternateKeysSkipsTablesWithoutSecondaryKeys(t *testing.T) {
	session := &Session{}
	tbuf := &TableBuffer{
		Table: &Table{BasicTable: &shared.BasicTable{Name: "customers"}},
	}

	err := session.mapAlternateKeys(putRowRequest{}, tbuf)

	require.NoError(t, err)
}
