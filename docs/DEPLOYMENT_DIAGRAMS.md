# Deployment Diagrams

These diagrams describe the current QuantaStream deployment topologies. The
mode names match existing scripts, SQLRunner engine flags, and compatibility
profiles. For prose guidance, see [DEPLOYMENT.md](DEPLOYMENT.md).

GitHub renders the diagrams from Mermaid source.

## QIAB Single-Node: `inabox-standard`

`inabox-standard` is the product-facing Quanta-in-a-Box shape. One
`quantastream` process owns the MySQL-compatible front door, SQL runtime,
in-process node adapter, catalog files, and local storage. It does not require
Consul.

```mermaid
flowchart LR
  mysql[MySQL clients<br/>mysql, Workbench, drivers]
  sqlrunner[SQLRunner<br/>inabox-standard]
  loader[quantastream-loader<br/>optional native ingest]

  subgraph host[Single host]
    qs[quantastream process]
    front[MySQL-compatible<br/>front door :4000]
    runtime[SQL planner<br/>and runtime]
    native[Native gRPC<br/>loader endpoint :4100]
    adapter[In-process<br/>local node adapter]
    storage[(Local data dir<br/>bitmap, BSI, KV, search)]
    catalog[(File-backed catalog<br/>config, CATALOG_OBJECTS, views)]
    wal[(Optional WAL<br/>and backup metadata)]

    qs --> front
    qs --> runtime
    qs --> native
    runtime --> adapter
    adapter --> storage
    runtime --> catalog
    runtime --> wal
  end

  mysql --> front
  sqlrunner --> front
  loader --> native
```

Operational notes:

- simplest local and small-deployment topology;
- one process lifecycle;
- no Consul requirement;
- no query-engine-to-node network hop;
- backup, WAL, and catalog files live with the local data directory.

## QIAB Direct-Cluster Harness: `inabox-direct`

`inabox-direct` is a development and conformance harness. SQLRunner hosts the
query engine in its own process and talks directly to a local data-node cluster
through the distributed gRPC path. There is no MySQL-compatible front door in
this topology.

```mermaid
flowchart LR
  dev[Developer shell]
  runner[SQLRunner<br/>-engine inabox-direct]

  subgraph runnerproc[SQLRunner process]
    planner[SQL planner<br/>and runtime]
    harness[Direct cluster<br/>runtime harness]
  end

  consul[(Consul<br/>discovery and catalog)]

  subgraph localhost[Single host, local cluster]
    node1[qs-server-1<br/>data node]
    node2[qs-server-2<br/>data node]
    node3[qs-server-3<br/>data node]
    data1[(node-1 data dir)]
    data2[(node-2 data dir)]
    data3[(node-3 data dir)]

    node1 --> data1
    node2 --> data2
    node3 --> data3
  end

  dev --> runner
  runner --> planner
  planner --> harness
  harness --> consul
  harness -- gRPC --> node1
  harness -- gRPC --> node2
  harness -- gRPC --> node3
  node1 --> consul
  node2 --> consul
  node3 --> consul
```

Operational notes:

- exercises distributed nodes, storage ownership, and Consul-backed metadata;
- avoids the MySQL wire path;
- useful for targeted SQLRuntime and node behavior debugging;
- not the user-facing QIAB product path.

## QIAB Local Distributed Shape: `inabox-local`

`inabox-local` keeps everything on one host but preserves the normal distributed
service boundary: MySQL clients talk to a local query front door, and the query
front door talks to local data nodes through Consul discovery and gRPC.

```mermaid
flowchart LR
  mysql[MySQL clients<br/>mysql, Workbench, drivers]
  sqlrunner[SQLRunner<br/>-engine inabox-local]
  loader[Loader or producer<br/>optional ingest]

  subgraph host[Single host]
    proxy[QuantaStream query process<br/>MySQL-compatible endpoint :4000]
    planner[SQL planner<br/>and runtime]
    consul[(Local Consul<br/>discovery and catalog)]

    subgraph nodes[Local data-node cluster]
      node1[qs-server-1]
      node2[qs-server-2]
      node3[qs-server-3]
    end

    data1[(node-1 data dir)]
    data2[(node-2 data dir)]
    data3[(node-3 data dir)]

    proxy --> planner
    planner --> consul
    planner -- gRPC --> node1
    planner -- gRPC --> node2
    planner -- gRPC --> node3
    node1 --> consul
    node2 --> consul
    node3 --> consul
    node1 --> data1
    node2 --> data2
    node3 --> data3
  end

  mysql --> proxy
  sqlrunner --> proxy
  loader --> proxy
```

Operational notes:

- validates the MySQL wire path plus distributed node RPC on a laptop;
- requires local Consul;
- useful for catalog propagation, service discovery, gRPC, and startup testing;
- does not validate multi-host networking or host replacement procedures.

## Full Distributed Mode

Full distributed mode separates query processors, data nodes, loaders, durable
storage, and service discovery across hosts. This is the scale-out topology
used for AWS benchmark and production-style validation.

```mermaid
flowchart LR
  clients[SQL clients<br/>apps, BI tools, SQLRunner]
  producers[Stream and batch producers]
  admin[qstream-admin<br/>ops and status]
  lb[Optional load balancer<br/>or DNS]

  subgraph querytier[Query tier]
    qp1[query processor 1<br/>MySQL endpoint :4000]
    qp2[query processor 2<br/>MySQL endpoint :4000]
  end

  subgraph ingesttier[Ingest tier]
    loader1[quantastream-loader]
    bulk[bulk loader<br/>TPC-H or archive replay]
  end

  consul[(Consul cluster<br/>discovery, catalog, coordination)]

  subgraph nodetier[Data-node tier]
    node1[qs-server-1]
    node2[qs-server-2]
    node3[qs-server-3]
    noden[qs-server-n]
  end

  store1[(durable storage<br/>node 1)]
  store2[(durable storage<br/>node 2)]
  store3[(durable storage<br/>node 3)]
  storen[(durable storage<br/>node n)]

  backup[(Backup target<br/>file, object store adapter, archive)]

  clients --> lb
  lb --> qp1
  lb --> qp2
  producers --> loader1
  producers --> bulk
  loader1 -- native or distributed writes --> node1
  loader1 -- native or distributed writes --> node2
  loader1 -- native or distributed writes --> node3
  loader1 -- native or distributed writes --> noden
  bulk -- direct cluster writes --> node1
  bulk -- direct cluster writes --> node2
  bulk -- direct cluster writes --> node3
  bulk -- direct cluster writes --> noden
  qp1 --> consul
  qp2 --> consul
  loader1 --> consul
  bulk --> consul
  node1 --> consul
  node2 --> consul
  node3 --> consul
  noden --> consul
  qp1 -- gRPC --> node1
  qp1 -- gRPC --> node2
  qp1 -- gRPC --> node3
  qp1 -- gRPC --> noden
  qp2 -- gRPC --> node1
  qp2 -- gRPC --> node2
  qp2 -- gRPC --> node3
  qp2 -- gRPC --> noden
  node1 --> store1
  node2 --> store2
  node3 --> store3
  noden --> storen
  admin --> consul
  admin --> node1
  admin --> node2
  admin --> node3
  admin --> noden
  store1 --> backup
  store2 --> backup
  store3 --> backup
  storen --> backup
```

Operational notes:

- query processors and loaders are horizontally deployable front-end services;
- data nodes own shards and durable node-local or attached storage;
- Consul currently provides service discovery and distributed catalog metadata;
- node identity and durable storage must be managed together;
- backup/restore, failover, node replacement, rebalancing, and rolling upgrade
  runbooks are operational gates for production-grade distributed deployments.
