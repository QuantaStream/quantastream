package source

// QuantaSource - Implementation of the data source interfaces for query processor.

import (
	//"database/sql/driver"
	//"fmt"
	"io/ioutil"
	"os"
	//"strings"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
	u "github.com/araddon/gou"
	"github.com/hashicorp/consul/api"
)

const (
	sourceType = "quanta"
)

// QuantaSource implements qlbridge `Source` to support Quanta indexes
// to have a Schema and implement and be operated on by Sql Statements.
type QuantaSource struct {
	exit     chan bool
	colIndex map[string]int

	// Quanta specific after here
	lastResultPos int
	baseDir       string
	sessionPool   *core.SessionPool
	clientConn    *shared.Conn
}

// NewQuantaSource - Construct a QuantaSource.
func NewQuantaSource(tableCache *core.TableCacheStruct, baseDir, consulAddr string, servicePort, sessionPoolSize int) (*QuantaSource, error) {

	m := &QuantaSource{}
	var err error
	var consulClient *api.Client
	if consulAddr != "" {
		consulClient, err = api.NewClient(&api.Config{Address: consulAddr})
		if err != nil {
			return m, err
		}
	}

	clientConn := shared.NewDefaultConnection("QuantaSource")
	clientConn.ServicePort = servicePort
	clientConn.Quorum = 3
	if err := clientConn.Connect(consulClient); err != nil {
		u.Error(err)
		os.Exit(1)
	}

	// Register for member leave/join notifications.
	clientConn.RegisterService(m)
	m.clientConn = clientConn

	//m.sessionPool = core.NewSessionPool(tableCache, clientConn, m.Schema, baseDir, sessionPoolSize)
	m.sessionPool = core.NewSessionPool(tableCache, clientConn, baseDir, sessionPoolSize)

	m.baseDir = baseDir
	if m.baseDir != "" {
		u.Infof("Constructing QuantaSource at baseDir '%s'", baseDir)
	}

	// name is a string and cols is an []string
	m.exit = make(chan bool, 1)

	return m, nil
}

// MemberLeft - Implements member leave notification due to failure.
func (m *QuantaSource) MemberLeft(nodeID string, index int) {

	u.Warnf("node %v left the cluster, purging sessions", nodeID)
	m.sessionPool.Recover(nil) // TODO: Need to re-evalute this when inserts are fully implemented.
}

// MemberJoined - A new node joined the cluster.
func (m *QuantaSource) MemberJoined(nodeID, ipAddress string, index int) {

	u.Warnf("node %v joined the cluster, purging sessions", nodeID)
	m.sessionPool.Recover(nil) // TODO: Need to re-evalute this when inserts are fully implemented.
}

// GetSessionPool - Return the underlying session pool instance.
func (m *QuantaSource) GetSessionPool() *core.SessionPool {
	return m.sessionPool
}

// GetConnection - Return the underlying client connection
func (m *QuantaSource) GetConnection() *shared.Conn {
	return m.clientConn
}

// Init initilize this db
func (m *QuantaSource) Init() {
}

// Close this source
func (m *QuantaSource) Close() error {

	if m.clientConn != nil {
		m.clientConn.StopPolling()
	}
	m.sessionPool.Shutdown()
	if m.clientConn != nil {
		if err := m.clientConn.Disconnect(); err != nil {
			return err
		}
	}
	defer func() { recover() }()
	close(m.exit)
	return nil
}

// Tables list
func (m *QuantaSource) Tables() []string {
	return m.ListTableNames()
}

// Columns - Return column name strings.
func (m *QuantaSource) Columns() []string {
	//return m.tbl.Columns()
	u.Debug("QuantaSource: Columns() called!")
	return nil
}

// ListTableNames - Return table name strings. from consul.
func (m *QuantaSource) ListTableNames() []string {

	if m.baseDir == "" {
		lock, errx := shared.Lock(m.sessionPool.AppHost.Consul, "admin-tool", "admin-tool")
		if errx != nil {
			u.Errorf("listTableNames: cannot obtain lock %v", errx)
			return []string{}
		}
		defer shared.Unlock(m.sessionPool.AppHost.Consul, lock)
		tables, errx := shared.GetTables(m.sessionPool.AppHost.Consul)
		if errx != nil {
			u.Errorf("shared.getTables failed: %v", errx)
			return []string{}
		}
		return tables
	}

	list := make([]string, 0)
	files, err := ioutil.ReadDir(m.baseDir)
	if err != nil {
		u.Error(err)
		os.Exit(1)
	}
	for _, f := range files {
		if f.IsDir() {
			list = append(list, f.Name())
		}
	}
	return list
}
