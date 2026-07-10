# Quanta Overview

*Quanta* is an open-source, generalized HTAP (Hybrid Transactional/Analytical Processing) database engine built on the Roaring Bitmap libraries. Designed as a highly performant alternative to traditional databases, *Quanta* emulates a subset of the MySQL networking protocol, providing compatibility with many MySQL drivers and tools. While it doesn’t support transactions or stored procedures, *Quanta* enables access to a wide ecosystem of MySQL-compatible resources and does support user-defined functions (UDFs).

The primary advantage of *Quanta* is its ability to provide subsecond access to large datasets with real-time updates. Data is compressed upon import and accessed directly in this format, allowing for high-performance querying on highly compressed data. Additionally, *Quanta* manages high cardinality strings by storing them in a distributed, persistent hashtable across Data Nodes. The architecture is similar to Apache Cassandra, allowing for both scalability and fault tolerance, with a future roadmap goal to enable active/active high availability and disaster recovery across multiple data centers.

## Architecture

The architecture of *Quanta* supports horizontal scalability, low-latency access, and efficient data ingestion and querying. Here are the core components:

- **Client Applications**: Applications connect to *Quanta* using industry-standard MySQL drivers, which communicate with the Query Processor via a Network Load Balancer.

- **Query Processor (Proxy)**: This component handles SQL queries from client applications. Multiple instances of the Query Processor are deployed for scalability, and a load balancer distributes MySQL connections across all instances. Each Query Processor can connect to all active Data Nodes, where data is transmitted as serialized byte arrays representing compressed bitmaps. The Query Processor re-hydrates these bitmaps and performs bitmap operations (e.g., AND, OR, difference) to deliver the final query response.

- **Data Nodes**: These nodes form the primary storage and processing layer, organized as a cluster to handle data ingestion and retrieval tasks. Data Nodes communicate with the Query Processor via gRPC, sending compact byte arrays to optimize network load. *Quanta* also stores high cardinality strings in a distributed hashtable across Data Nodes for efficient retrieval.

- **Kinesis Consumers**: The Kinesis Consumers ingest data streams from Amazon Kinesis, communicating with the Data Nodes via gRPC. They transform incoming data into bitmaps, buffer and aggregate them with OR operations, and then send the results to the appropriate Data Nodes for storage. This approach allows *Quanta* to pre-aggregate data efficiently before it reaches the Data Nodes.

- **Consul (Service Discovery and Metadata Storage)**: Consul enables service discovery by identifying the network endpoints of active Data Nodes, which are then accessible to upstream components like the Query Processors and Kinesis Consumers. Consul also leverages a key/value store to manage schema metadata for tables and fields, enabling consistent access to schema information across the system.

## Roadmap

*Quanta*'s roadmap focuses on expanding SQL capabilities, scalability, and performance optimization. Key goals include:

1. **Enhanced SQL Support**: Adding support for SQL features like GROUP BY, HAVING clauses, and multiple aggregations in the SELECT list to enable complex analytical queries, particularly for TPC-H benchmarking.
   
2. **Autoscaling and Resource Optimization**: Developing an autoscaler to dynamically add or remove Data Nodes based on workload, with metrics-driven scaling to manage resource use efficiently.

3. **Optimized Data Distribution and Load Balancing**: Improving the data distribution strategy based on resource utilization. Dynamic data distribution will help balance load across nodes more effectively.

4. **Active/Active HA/DR**: Implementing active/active high availability and disaster recovery across multiple data centers for improved resilience.

5. **Conflict Resolution and Time Synchronization**: Adding conflict resolution strategies using vector clocks or version vectors. With AWS’s Time Sync service, *Quanta* aims to eventually support microsecond-level time synchronization for conflict management.

6. **GPU-Accelerated Bitmap Processing**: Exploring GPU acceleration for core bitmap operations to improve performance on large-scale data processing tasks.


# Requirements 

Go version 1.14.14 or later.
HashiCorp Consul 1.4.x or later.


# Getting Started

[A Quick Start Guide can be found here](https://github.com/QuantaStream/quantastream/tree/master/test/README.md)

# Build and Deployment

[Build and Deployment Instructions](https://github.com/QuantaStream/quantastream/tree/master/Docker/README.md)

# Configuration Documentation
[Schema File Configuration Docs](https://github.com/QuantaStream/quantastream/tree/master/configuration/README.md)

# Terminology
[Quanta Glossary](docs/GLOSSARY.md)

# Tool and Driver compatibility
* MySQL command line client (5.7.0)
* Java JDBC driver (8.0.11)
* Python MySQL connector
* Node.js MySQL driver
* MySQL Workbench (coming soon!)

# Road Map
The current version is 0.8 and is currently in "alpha" state.

## Version 0.9
This version once release will be considered "beta" and will include the following:
* Cluster administration/monitoring tools and API.
* RBAC interfaces.


## Version 1.0 - Quanta-in-a-box community edition.
* Support for SQL Views.
* Support for SQL Subqueries.
* Support for temporary tables.
* Hierarchic (non-Star Schema) joins.
* Drop-in MySql support
* Both batch and steaming data load with queries against live cluster.
* Competitive TPC-H benchmark.
* Basic backup/recovery.
* Production readiness.

## Version 2.0 - Broad scaling and enterprise readiness.
* Intermediate results caching.
- Full support for multi-node deployment
- Replication, failover, HA/DR
- Scale out/in of a live cluster.  Ability to add/remove nodes which shard re-distibution.
- Enterprise grade security, standards based authentication integration, RBAC.

## Version 3.0 - Update anywhere across geographically distributed data centers.
- Enhanced monitoring and management APIs.
- Active/Active multi data center support.
- Automated cluster management leveraging AI.




# Issues

The process for reporting bugs is as follows:

1. Write a unit test that reproduces the issue.
2. Create a branch off of develop containing the test case.
3. Submit a pull request.


# Contributing

Contributions are always welcome.  The process is straightforward:

1. Create your feature branch off of the develop branch (git checkout -b my-new-feature)
2. Write Tests!
3. Make sure the codebase adhere to the Go coding standards by executing `gofmt -s -w ./` followed by `golint`
4. Commit your changes (git commit -am 'Add some feature')
5. Push to the branch (git push origin my-new-feature)
6. Create new Pull Request.
