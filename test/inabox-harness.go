package test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	//"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/core"
	admin "github.com/QuantaStream/quantastream/quanta-admin-lib"
	"github.com/QuantaStream/quantastream/server"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/source"
	u "github.com/araddon/gou"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
)

// some tests start a cluster and must listen on port 4000
// this is a mutex to ensure that only one test at a time can listen on port 4000
var AcquirePort4000 sync.Mutex

var ConsulAddress = "127.0.0.1:8500" // also used by sqlrunner main.

// tests will time out so run like this:
// go test -timeout 10m

func StartNode(nodeStart int) (*server.Node, error) {

	Version := "v0.0.1"
	Build := "2006-01-01"

	environment := "DEV"
	logLevel := "DEBUG"

	shared.InitLogging(logLevel, environment, "Data-Node", Version, "Quanta")

	index := nodeStart
	{
		hashKey := "quanta-node-" + strconv.Itoa(index)
		dataDir := "../test/localClusterData/" + hashKey + "/data"
		bindAddr := "127.0.0.1"
		port := 4010 + index

		WaitForPort(port)

		consul := bindAddr + ":8500"

		// Create /bitmap data directory
		fmt.Printf("Creating bitmap data directory: %s \n", dataDir+"/bitmap")
		if _, err := os.Stat(dataDir + "/bitmap"); err != nil {
			err = os.MkdirAll(dataDir+"/bitmap", 0777)
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}

		u.Infof("Node identifier '%s'", hashKey)

		u.Infof("Connecting to Consul at: [%s] ...\n", consul)
		consulClient, err := api.NewClient(&api.Config{Address: consul})
		if err != nil {
			u.Errorf("Is the consul agent running?")
			log.Fatalf("[node: Cannot initialize endpoint config: error: %s", err)
		}

		newNodeName := hashKey
		// newNodeName = "quanta"
		m, err := server.NewNode(fmt.Sprintf("%v:%v", Version, Build), int(port), bindAddr, dataDir, newNodeName, consulClient)
		if err != nil {
			u.Errorf("[node: Cannot initialize node config: error: %s", err)
		}
		m.ServiceName = "quanta" // not hashKey. Do we need this? (atw)
		m.IsLocalCluster = true

		kvStore := server.NewKVStore(m)
		m.AddNodeService(kvStore)

		search := server.NewStringSearch(m)
		m.AddNodeService(search)

		bitmapIndex := server.NewBitmapIndex(m)
		m.AddNodeService(bitmapIndex)

		// load the table schema from the file system manually here
		//  eg.
		// table := "cities"
		// shared.LoadSchema("../test/testdata/config", table, consulClient)
		// Start listening endpoint
		m.Start()

		start := time.Now()
		err = m.InitServices()
		elapsed := time.Since(start)
		if err != nil {
			log.Printf("InitServices FAIL ERROR. %v", err)
			log.Fatal(err)
		}
		log.Printf("Node initialized in %v.", elapsed)

		go func() {
			joinName := "quanta"   // this is the name for a cluster of nodes was "quanta-node"
			err = m.Join(joinName) // this does not return
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
			fmt.Println("StartNodes returned from join")

			<-m.Stop
			fmt.Println(hashKey, "harness StartNodes got m.Stop")
			m.Leave()
			select {
			case err = <-m.Err:
			default:
			}
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}()
		return m, nil
	}
}

type LocalProxyControl struct {
	Stop            chan bool
	Src             *source.QuantaSource
	StopSchemaWatch func()
}

func StartProxy(count int, testConfigPath string) *LocalProxyControl {

	WaitForPort(4000)

	localProxy := &LocalProxyControl{}

	localProxy.Stop = make(chan bool)

	fmt.Println("Starting proxy")

	//proxy.SetupCounters()
	//proxy.Init()

	logging := "DEBUG"
	environment := "DEV"
	Version := "1.0.0"
	//proxy.ConsulAddr = "127.0.0.1:8500"
	//proxy.IsLocalCluster = true
	// cognito url for token service publicKeyURL := "" // unused
	//proxy.QuantaPort = 4010
	//proxyHostPort := 4000

	// region := "us-east-1"

	if strings.ToUpper(logging) == "DEBUG" || strings.ToUpper(logging) == "TRACE" {
		if strings.ToUpper(logging) == "TRACE" {
			//expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		shared.InitLogging(logging, environment, "Proxy", Version, "Quanta")
	}

	//log.Printf("Connecting to Consul at: [%s] ...\n", proxy.ConsulAddr)
	//consulConfig := &api.Config{Address: proxy.ConsulAddr}
	//consulConfig := &api.Config{Address: "127.0.0.1:8500"}
	/*
		stopSchemaWatch, errx := shared.RegisterSchemaChangeListenerWithStop(consulConfig, proxy.SchemaChangeListener)
		if errx != nil {
			u.Error(errx)
			os.Exit(1)
		}
		localProxy.StopSchemaWatch = stopSchemaWatch
	*/

	fmt.Println("Proxy RegisterSchemaChangeListener done")

	poolSize := 4

	// If the pool size is not configured then set it to the number of available CPUs
	// this is weird atw
	sessionPoolSize := poolSize
	if sessionPoolSize == 0 {
		sessionPoolSize = runtime.NumCPU()
		log.Printf("Session Pool Size not set, defaulting to number of available CPUs = %d", sessionPoolSize)
	} else {
		log.Printf("Session Pool Size = %d", sessionPoolSize)
	}

	fmt.Println("Proxy before NewQuantaSource")

	// Construct Quanta source
	tableCache := core.NewTableCacheStruct() // is this right? delete this?

	// configDir := "../test/testdata" // gets: ../test/testdata/config/schema.yaml
	// FIXME: empty configDir panics
	configDir := testConfigPath
	var err error
	localProxy.Src, err = source.NewQuantaSource(tableCache, configDir, "127.0.0.1:8500", 4010, sessionPoolSize)
	if err != nil {
		u.Error(err)
	}
	fmt.Println("Proxy after NewQuantaSource")

	fmt.Println("Proxy startup. ")

	// Start server endpoint

	// TODO: Init and start

	go func(localProxy *LocalProxyControl) {

		for range localProxy.Stop {
			fmt.Println("Stopping proxy")
			if localProxy.StopSchemaWatch != nil {
				localProxy.StopSchemaWatch()
			}
			// TODO: Shutdown code here
			localProxy.Src.Close()
		}

	}(localProxy)

	return localProxy
}

func IsConsuleRunning() bool {
	_, err := http.Get("http://localhost:8500/v1/health/service/quanta") // was quanta-node
	return err == nil
}

func IsLocalRunning() bool {

	result := "[]"
	res, err := http.Get("http://localhost:8500/v1/health/service/quanta") // was quanta-node
	if err == nil {
		resBody, err := io.ReadAll(res.Body)
		if err == nil {
			result = string(resBody)
		}
	} else {
		fmt.Println("is consul not running?", err)
	}
	// fmt.Println("result:", result)

	// the three cases are 1) what cluster? 2) used to have one but now its critical 3) Here's the health.
	isNotRunning := strings.HasPrefix(result, "[]") || strings.Contains(result, "critical")

	return !isNotRunning
}

// captureStdout calls a function f and returns its stdout side-effect as string
func captureStdout(f func()) string {
	// return to original state afterwards
	// note: defer evaluates (and saves) function ARGUMENT values at definition
	// time, so the original value of os.Stdout is preserved before it is changed
	// further into this function.
	defer func(orig *os.File) {
		os.Stdout = orig
	}(os.Stdout)

	r, w, _ := os.Pipe()
	os.Stdout = w
	f()
	w.Close()
	out, _ := io.ReadAll(r)

	return string(out)
}

// GetStatusLocal calls the admin status command and returns the output
// It starts a client and then forms the status from maps in the Connection.
func GetStatusLocal(ctx *admin.Context) string {
	statusCmd := admin.StatusCmd{}
	out := captureStdout(func() {
		statusCmd.Run(ctx)
	})
	return out
}

// GetStatusViaAdminLocal calls the admin status command and returns the output
// it makes a new connection and then sends grpc to all the nodes.
func GetStatusViaAdminLocal() string {
	consulAddress := "127.0.0.1:8500"
	ctx := admin.Context{ConsulAddr: consulAddress,
		Port:  4000,
		Debug: true}

	out := GetStatusLocal(&ctx)

	// see below for example output
	state := strings.Split(out, "Cluster State = ")
	if len(state) == 2 {
		if strings.HasPrefix(state[1], "GREEN") {
			return "GREEN"
		}
	}
	return "bad"
}

// WaitForStatusGreenLocal does an admin status command and waits for the cluster to be green
// It could just check the proxy status but this is more general.
func WaitForStatusGreenLocal() {

	consulAddress := "127.0.0.1:8500"

	ctx := admin.Context{ConsulAddr: consulAddress,
		Port:  4000,
		Debug: true}

	now := time.Now()
	for {
		if time.Since(now) > time.Second*90 { // syncing could take a while
			log.Fatal("consul timeout driver after WaitForStatusGreenLocal")
		}

		out := GetStatusLocal(&ctx)

		fmt.Println("status", out)
		// eg   Connecting to Consul at: [172.20.0.2:8500] ...
		//  	Connecting to Quanta services at port: [4000] ...
		//		ADDRESS            STATUS    DATA CENTER                              SHARDS   MEMORY   VERSION
		//      172.20.0.3         Active    dc1                                           0   0        :
		//		172.20.0.4         Syncing   dc1                                           0   0        :
		//		172.20.0.5         Active    dc1                                           0   0        :
		//		172.20.0.6         Active    dc1                                           0   0        :
		//
		//		Cluster State = GREEN, Active nodes = 3, Target Cluster Size = 3
		//
		state := strings.Split(out, "Cluster State = ")
		if len(state) == 2 {
			if strings.HasPrefix(state[1], "GREEN") {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func WaitForStatusGreen(consulAddress string, nodeName string) {

	// docker exec admin --consul-addr 172.20.0.2:8500  status

	now := time.Now()
	for {
		if time.Since(now) > time.Second*90 { // syncing could take a while
			log.Fatal("consul timeout driver after WaitForStatusGreen")
		}

		cmd := "docker exec " + nodeName + " admin --consul-addr " + consulAddress + " status"
		out, err := Shell(cmd, "")
		_ = err
		// fmt.Println("status", out, err)
		// eg   Connecting to Consul at: [172.20.0.2:8500] ...
		//  	Connecting to Quanta services at port: [4000] ...
		//		ADDRESS            STATUS    DATA CENTER                              SHARDS   MEMORY   VERSION
		//      172.20.0.3         Active    dc1                                           0   0        :
		//		172.20.0.4         Syncing   dc1                                           0   0        :
		//		172.20.0.5         Active    dc1                                           0   0        :
		//		172.20.0.6         Active    dc1                                           0   0        :
		//
		//		Cluster State = GREEN, Active nodes = 3, Target Cluster Size = 3
		//
		state := strings.Split(out, "Cluster State = ")
		if len(state) == 2 {
			if strings.HasPrefix(state[1], "GREEN") {
				return
			}
		}

		time.Sleep(500 * time.Millisecond)
	}
}

type ClusterLocalState struct {
	nodes               []*server.Node
	ProxyControl        *LocalProxyControl
	weStartedTheCluster bool
	ProxyConnect        *ProxyConnectStrings // for sql runner
	Db                  *sql.DB
}

func StartNodes(state *ClusterLocalState, count int) {

	nodes := make([]*server.Node, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		index := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, err := StartNode(index)
			nodes[index] = node
			errs[index] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		check(err)
	}
	state.nodes = nodes
}

func (state *ClusterLocalState) StopNodes() {

	var wg sync.WaitGroup
	for _, node := range state.nodes {
		if node == nil {
			continue
		}
		n := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			cx := context.Background()
			_, err := n.Shutdown(cx, &empty.Empty{})
			check(err)
		}()
	}
	wg.Wait()
}

// CheckForClosedPort returns true if the port is closed.git
func CheckForClosedPort(port string) bool {
	timeout := time.Second
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), timeout)
	if err != nil {
		// fmt.Println("Port is closed:", err)
		return true // it's closed
	}
	if conn != nil {
		defer conn.Close()
		// fmt.Println("Port is open", net.JoinHostPort("127.0.0.1", port))
	}
	return false // it's still in use.
}

func WaitForPort(port int) {
	start := time.Now()
	for {
		if CheckForClosedPort(strconv.Itoa(port)) {
			break
		}
		if time.Since(start) > 2*time.Minute {
			fmt.Println("ERROR WaitForPort timed out waiting for port to become available.")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (state *ClusterLocalState) Release() {
	if state.weStartedTheCluster {
		state.ProxyControl.Stop <- true
		time.Sleep(100 * time.Millisecond)
		state.StopNodes()
		time.Sleep(100 * time.Millisecond)
	}
}

func WaitForLocalActive(state *ClusterLocalState) {

	allActive := true
	for {
		allActive = true
		for _, node := range state.nodes {
			if node.State != server.Active {
				fmt.Println("WaitForLocalActive node not active...", node.GetNodeID(), node.State)
				allActive = false
			}
		}
		if allActive {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("WaitForLocalActive allActive", allActive)
}

func WaitForConsulPassingLocal(consulClient *api.Client, count int) {
	start := time.Now()
	for {
		if time.Since(start) > 90*time.Second {
			log.Fatal("consul timeout waiting for local quanta health checks")
		}

		serviceEntries, _, err := consulClient.Health().Service("quanta", "", false, nil)
		if err != nil {
			fmt.Println("WaitForConsulPassingLocal health error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		passing := 0
		for _, entry := range serviceEntries {
			if entry.Service == nil || entry.Service.ID == "shutdown" {
				continue
			}
			allChecksPassing := len(entry.Checks) > 0
			for _, check := range entry.Checks {
				if check.Status != "passing" {
					allChecksPassing = false
					break
				}
			}
			if allChecksPassing {
				passing++
			}
		}
		if passing >= count {
			fmt.Println("WaitForConsulPassingLocal passing", passing)
			return
		}
		fmt.Println("WaitForConsulPassingLocal waiting passing", passing, "target", count)
		time.Sleep(500 * time.Millisecond)
	}
}

func Ensure_this_cluster(count int, state *ClusterLocalState) {
	local_Ensure_cluster(count, state)
}

func Ensure_cluster(count int) *ClusterLocalState {
	var state = &ClusterLocalState{}
	local_Ensure_cluster(count, state)
	return state
}

// Ensure_cluster checks to see if there already is a cluster and
// starts a local one as needed.
// This depends on having consul on port 8500 in a terminal
func local_Ensure_cluster(count int, state *ClusterLocalState) {

	var proxyConnect ProxyConnectStrings
	proxyConnect.Host = "127.0.0.1"
	proxyConnect.User = "MOLIG004"
	proxyConnect.Password = ""
	proxyConnect.Port = "4000"
	proxyConnect.Database = "quanta"

	state.ProxyConnect = &proxyConnect

	// set the quorum size to 3
	localConsulAddress := "127.0.0.1"
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, count)
	check(err)

	isNotRunning := !IsLocalRunning()
	if isNotRunning {

		shared.StartPprofAndPromListener("true")

		// start the cluster
		StartNodes(state, count)

		WaitForLocalActive(state)
		WaitForConsulPassingLocal(consulClient, count)

		configDir := ""
		state.ProxyControl = StartProxy(1, configDir)

		sharedKV := shared.NewKVStore(state.ProxyControl.Src.GetConnection())

		// need to sort this out and just have one

		fmt.Println("consul status ", sharedKV.Consul.Status())

		state.weStartedTheCluster = true
	} else {
		state.weStartedTheCluster = false
	}

	state.Db, err = state.ProxyConnect.ProxyConnectConnect()
	check(err)

	//return state
}

func MergeSqlInfo(total *SqlInfo, got SqlInfo) {
	total.ExpectedRowcount += got.ExpectedRowcount
	total.ActualRowCount += got.ActualRowCount
	total.ExpectError = got.ExpectError
	if len(got.ErrorText) > 0 && !got.ExpectError {
		total.ErrorText += "\n" + got.ErrorText
		fmt.Println("got.ErrorText", got.ErrorText)
	}
	if got.ExpectedRowcount != got.ActualRowCount {
		total.FailedChildren = append(total.FailedChildren, got)
	}
}

func ExecuteSqlFile(state *ClusterLocalState, filename string) SqlInfo {
	bytes, err := os.ReadFile(filename)
	check(err)
	sql := string(bytes)
	lines := strings.Split(sql, "\n")
	var total SqlInfo
	total.Statement = ""
	hadSelect := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !hadSelect {
			if strings.HasPrefix(strings.ToLower(line), "select ") {
				hadSelect = true
				// time.Sleep(5 * time.Second) // For experiments only.
			}
		}
		lineLines := strings.Split(line, "\\") // '\' is a line continuation
		got := AnalyzeRow(*state.ProxyConnect, lineLines, true)
		MergeSqlInfo(&total, got)
	}
	return total
}

func check(err error) {
	if err != nil {
		fmt.Println("ERROR ERROR check err", err)
		// panic(err.Error())
	}
}
