# AWS Distributed Ops

These scripts are bench-runner-oriented helpers for the current distributed
benchmark environment. They assume bench-runner runs the Consul server and
`quantastream-proxy`, while each QS server runs a local Consul client and a
`quantastream-node` service.

Copy the env template once and edit it for the active fleet:

```sh
cd ~/quantastream
cp startup-scripts/aws/quantastream-aws.env.example startup-scripts/aws/quantastream-aws.env
vi startup-scripts/aws/quantastream-aws.env
```

Deploy or update services:

```sh
./startup-scripts/aws/deploy-distributed.sh --pull
```

Check health:

```sh
./startup-scripts/aws/cluster-health.sh --wait=180
```

Verify every QS node can fast-forward from GitHub:

```sh
./startup-scripts/aws/git-pull-nodes.sh
```

Sync TPC-H schema artifacts into Consul:

```sh
./startup-scripts/aws/sync-schema.sh
```

Load an empty, fully joined cluster:

```sh
./startup-scripts/aws/load-tpch-distributed.sh
```

Run the compact read-only suite:

```sh
./startup-scripts/aws/run-readonly-benchmark.sh \
  --profile aws-distributed-sf005-readonly \
  --report /tmp/aws-distributed-sf005-readonly.json
```

Run any SQLRunner suite or case:

```sh
./startup-scripts/aws/run-sqlrunner.sh \
  --suite ../tpc-h-benchmark/sqltests/tpch_profile_scale.yaml \
  --case tpch_profile_scale.q5.formal_revenue \
  --profile aws-q5-formal \
  --runs 5 \
  --summary
```

Restart or inspect services:

```sh
./startup-scripts/aws/services.sh restart
./startup-scripts/aws/services.sh status
./startup-scripts/aws/services.sh logs --nodes-only --lines=80
```

For correctness benchmarks, start every target data node empty, verify the full
target cluster is GREEN, then load through the distributed path. Online
scale-out from already loaded data is future 2.0 work.
