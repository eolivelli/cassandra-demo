# Cassandra Online Store Simulator

A small Java 17 command-line tool that simulates a **real-world online computer store**
workload (laptops, monitors, mice, keyboards, GPUs, …) on **Apache Cassandra 5**.

It is built as a load/behaviour generator: a timer produces "events" that are executed
by a configurable thread pool (default parallelism **64**), and is designed to be driven
up to **hundreds of millions of operations**.

## What it exercises

The schema and the workload deliberately cover a broad slice of Cassandra 5:

| Area | Used |
| --- | --- |
| Statement kinds | standard mutations, **LWTs** (`IF NOT EXISTS`, compare-and-set), **logged** batches, **unlogged** batches, reads |
| Mutation kinds | `INSERT`, `UPDATE`, `DELETE`, plus **partial** updates/deletes (single map entry, single column, collection add/remove, single UDT field replacement) |
| Data types | tinyint, smallint, int, bigint, varint, decimal, float, double, boolean, text, ascii, uuid, timeuuid, timestamp, date, time, inet, duration, blob |
| UDTs | `address`, `phone`, `dimensions`, `order_item`, `tracking_event` — used both frozen and inside collections |
| Collections | frozen and non-frozen `list` / `set` / `map` |
| Keys | partition + clustering columns with explicit clustering order |
| Consistency | every operation runs at **QUORUM** (configurable); serial consistency `SERIAL` for LWTs |

By design it uses **no counters** and **no static columns**, and **no vector / geo** types.

## Data model (`store` keyspace)

- **`customers`** — one partition per customer. The "complex personal information page":
  many scalar columns, a `default_address` (frozen UDT), an `addresses` list of frozen
  UDTs, a `phones` set of frozen UDTs, frozen and non-frozen maps/sets, a `blob`, etc.
- **`products`** — partitioned by `category`, clustered by `product_id`. Carries frozen
  `dimensions`, non-frozen attribute maps/lists/sets, a frozen `specs` map and a frozen
  `warehouse_codes` set, plus `stock`/`reserved` used by the compare-and-set LWT.
- **`orders`** — partitioned by `customer_id`, clustered by `order_id` (`timeuuid`, newest
  first). Holds the basket (`items`, a list of frozen `order_item`) and **delivery
  tracking** (`tracking`, a non-frozen list of frozen `tracking_event`).

See [`SchemaManager`](src/main/java/com/example/store/schema/SchemaManager.java) for the
full DDL.

## The events

The [`StoreOperations`](src/main/java/com/example/store/sim/StoreOperations.java) class
implements weighted-random events. Each event records exactly one operation in the stats:

| Event | What it does | API highlight |
| --- | --- | --- |
| `create_customer` | insert a full customer profile | `INSERT … IF NOT EXISTS` (LWT) |
| `create_product` | insert a catalogue product | standard insert |
| `update_customer` | one of: set a `preferences[k]`, append an address, add tags, add a phone, set scalars, replace `default_address` | **partial** updates |
| `delete_customer` | one of: delete a `preferences[k]`, delete columns, remove a tag | **partial** deletes |
| `place_order` | read products, build a basket, insert the order and update the customer totals | **logged batch** + read |
| `update_tracking` | read a recent order, append a `tracking_event`, update status | read + non-frozen collection append |
| `cancel_order` | cancel an order unless already delivered | **LWT** (`IF delivered = false`) |
| `reserve_stock` | read stock then decrement it safely | read + **compare-and-set LWT** |
| `bulk_product_update` | reprice several products in one partition | **unlogged batch** |

Recently created customer/product ids are kept in bounded ring buffers
([`BoundedIdPool`](src/main/java/com/example/store/sim/BoundedIdPool.java)) so later events
target rows that really exist, without unbounded memory growth.

## Build

```bash
mvn package
```

This produces a runnable fat jar at `target/cassandra-store-sim.jar`.

## Usage

A convenience wrapper, [`store.sh`](store.sh), runs the built jar and passes all
arguments straight through (it assumes `mvn package` has been run and `java` is on the
PATH):

```bash
./store.sh --help
./store.sh create-schema -k store --replication-factor 3
./store.sh create-indexes -k store -t "CREATE CUSTOM INDEX %s ON %s.%s (email) USING 'StorageAttachedIndex'"
./store.sh simulate -k store --parallelism 64 --operations 100000000
./store.sh drop-schema -k store
```

### Creating indexes (optional phase)

`create-indexes` runs one `CREATE INDEX` per table from a template you supply. The
template must contain exactly **three `%s` placeholders**, filled in this order:

1. the index name, computed automatically as `index_<table>` (e.g. `index_customers`),
2. the keyspace name,
3. the table name.

Any other literal `%` in the template (for example inside `WITH OPTIONS`) must be escaped
as `%%`.

```bash
./store.sh create-indexes -k store \
    -t "CREATE CUSTOM INDEX %s ON %s.%s (email) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive':'false'}"
```

This iterates over the tables (`customers`, `products`, `orders`) and executes, e.g.:

```sql
CREATE CUSTOM INDEX index_customers ON store.customers (email) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive':'false'}
```

The same phase can be run as part of `simulate` with `--create-indexes --index-template "…"`
(and `--create-schema` before it, if needed). Because a single template is applied to
every table, make sure the column(s) it references exist on all of them.

The equivalent explicit invocations:

```bash
# 1. Create the schema (RF configurable)
java -jar target/cassandra-store-sim.jar create-schema \
    -H 127.0.0.1 -P 9042 -d datacenter1 -k store --replication-factor 3

# 2. Run the simulation
java -jar target/cassandra-store-sim.jar simulate \
    -H 127.0.0.1 -k store \
    --parallelism 64 \
    --operations 100000000 \
    --rate 0                # 0 = as fast as possible

# Drop everything
java -jar target/cassandra-store-sim.jar drop-schema -k store
```

`simulate --create-schema --replication-factor N` will create the schema first as a
convenience.

### Key tunables (`simulate`)

| Option | Default | Meaning |
| --- | --- | --- |
| `-p, --parallelism` | 64 | worker threads executing events |
| `-n, --operations` | 100000000 | total operations (0 = unlimited) |
| `--duration-seconds` | 0 | stop after N seconds (0 = no limit) |
| `--rate` | 0 | target ops/sec (0 = unbounded, back-pressured) |
| `--tick-millis` | 10 | producer tick interval |
| `--customer-pool` / `--product-pool` | 100000 / 50000 | recent ids kept for reuse |
| `--categories` | built-in list | comma-separated product categories |
| `-c, --consistency` | QUORUM | consistency level for every operation |
| `--replication-factor` | 1 | RF (for `create-schema`, or `simulate --create-schema`) |
| `--create-indexes` | off | create indexes before running (needs `--index-template`) |
| `-t, --index-template` | — | `CREATE INDEX` template with three `%s` (index, keyspace, table) |
| `--pool-connections` | 4 | driver connections per node |

Connection options (`-H/--host`, `-P/--port`, `-d/--datacenter`, `-k/--keyspace`,
`-c/--consistency`) are shared by every sub-command.

## Tests

A [Testcontainers](https://testcontainers.org) integration test boots a real
**`cassandra:5.0`** container, creates the schema, runs a short simulation and asserts that
customers, products and orders were produced, then drops the schema.

```bash
mvn test
```

> **Docker API note:** Docker Engine 29+ requires API version ≥ 1.44, while the
> docker-java client bundled with Testcontainers probes at 1.32. The build pins the API
> version via the Surefire configuration (`dockerApiVersion`, default `1.44`). Override
> with `-DdockerApiVersion=1.47` if your engine needs a different minimum.

## Requirements

- JDK 17+ (compiled with `--release 17`; tested on a JDK 25 toolchain)
- Maven 3.9+
- Docker (only for the integration test)
- A reachable Cassandra 5 cluster (for `create-schema` / `simulate` / `drop-schema`)
