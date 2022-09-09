[![Java CI with Maven](https://github.com/jerrinot/flink-connector-questdb/actions/workflows/ci.yml/badge.svg)](https://github.com/jerrinot/flink-connector-questdb/actions/workflows/ci.yml)
[![Maven](https://maven-badges.herokuapp.com/maven-central/info.jerrinot/flink-connector-questdb/badge.png)](https://maven-badges.herokuapp.com/maven-central/info.jerrinot/flink-connector-questdb)

# ⚠️ Deprecation Notice ⚠️
### This repository is no longer maintained. The project has been transfered to the [QuestDB organization](https://github.com/questdb/flink-questdb-connector) and it's maintained there.


# flink-connector-questdb
Apache Flink Table &amp; SQL Connector for QuestDB

[Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/overview/) Sink for QuestDB via [ILP protocol](https://questdb.io/docs/develop/insert-data#influxdb-line-protocol). 

## Example
1. Start [local QuestDB](https://questdb.io/docs/get-started/docker)
2. Start [install and start Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/local_installation/)
3. Start Apache Flink [SQL Console](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sqlclient/)
4. Create a dynamic table in Flink SQL Console:
```sql
CREATE TABLE questTable (a BIGINT NOT NULL, b STRING) WITH (
'connector'='questdb',
'host'='localhost',
'table'='flink_table'
)
```
5. Insert an item into the table created in the previous step:
```sql
insert into questTable (a, b) values (42, 'foobar');
```
6. Flink should create a table `flink_table` in QuestDB and eventually insert a new row into it. 

TODO: Connector installation!
