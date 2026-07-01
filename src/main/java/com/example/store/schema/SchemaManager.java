package com.example.store.schema;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Creates and drops the schema for the online-computer-store scenario.
 *
 * <p>The schema deliberately exercises a wide range of Cassandra 5 features:
 * <ul>
 *   <li>most scalar types (tinyint, smallint, int, bigint, varint, decimal, float,
 *       double, boolean, text, ascii, uuid, timeuuid, timestamp, date, time, inet,
 *       duration, blob) &mdash; but no vector and no geo types;</li>
 *   <li>user defined types, both frozen and embedded in collections;</li>
 *   <li>frozen and non-frozen collections (list/set/map);</li>
 *   <li>clustering columns with explicit clustering order;</li>
 *   <li>no counters and no static columns, by design.</li>
 * </ul>
 */
public final class SchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

    /** The tables managed by this schema, in creation order. */
    public static final List<String> TABLE_NAMES = List.of("customers", "products", "orders");

    private final CqlSession session;
    private final String keyspace;

    public SchemaManager(CqlSession session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    public void createKeyspace(int replicationFactor) {
        String cql = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class':'SimpleStrategy','replication_factor':" + replicationFactor + "}";
        LOG.info("Creating keyspace {} (RF={})", keyspace, replicationFactor);
        session.execute(cql);
    }

    /** Creates the keyspace, the user defined types and the tables. Idempotent. */
    public void createSchema(int replicationFactor) {
        createKeyspace(replicationFactor);

        for (String ddl : userTypes()) {
            session.execute(ddl);
        }
        for (String ddl : tables()) {
            session.execute(ddl);
        }
        LOG.info("Schema created in keyspace {}", keyspace);
    }

    /**
     * Optional "create indexes" phase: for every table, format the supplied template and
     * run it. The template must contain exactly three {@code %s} placeholders, filled in
     * this order: the index name (computed automatically as {@code index_<table>}), the
     * keyspace name, and the table name. For example:
     *
     * <pre>
     *   CREATE CUSTOM INDEX %s ON %s.%s (email) USING 'StorageAttachedIndex' WITH OPTIONS = {...}
     * </pre>
     *
     * Any other {@code %} in the template must be escaped as {@code %%}.
     */
    public void createIndexes(String template) {
        for (String cql : buildIndexStatements(template)) {
            LOG.info("Creating index: {}", cql);
            session.execute(cql);
        }
        LOG.info("Indexes created in keyspace {}", keyspace);
    }

    /**
     * Renders the index template once per table, without touching Cassandra. The template
     * placeholders are filled in the order: index name ({@code index_<table>}), keyspace,
     * table.
     */
    public List<String> buildIndexStatements(String template) {
        List<String> statements = new java.util.ArrayList<>(TABLE_NAMES.size());
        for (String table : TABLE_NAMES) {
            String indexName = "index_" + table;
            try {
                statements.add(String.format(template, indexName, keyspace, table));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid index template. It must contain exactly three %s placeholders " +
                                "(index name, keyspace, table) and escape any literal % as %%. Template was: "
                                + template, e);
            }
        }
        return statements;
    }

    /** Drops the entire keyspace. */
    public void dropSchema() {
        LOG.info("Dropping keyspace {}", keyspace);
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    private List<String> userTypes() {
        String ks = keyspace + ".";
        return List.of(
                // A postal address; embedded (frozen) inside customer / order rows.
                "CREATE TYPE IF NOT EXISTS " + ks + "address (" +
                        "  label text," +
                        "  street text," +
                        "  city text," +
                        "  state text," +
                        "  zip text," +
                        "  country ascii," +
                        "  lat double," +
                        "  lon double" +
                        ")",

                "CREATE TYPE IF NOT EXISTS " + ks + "phone (" +
                        "  country_code smallint," +
                        "  number text," +
                        "  kind text," +
                        "  verified boolean" +
                        ")",

                "CREATE TYPE IF NOT EXISTS " + ks + "dimensions (" +
                        "  width_cm decimal," +
                        "  height_cm decimal," +
                        "  depth_cm decimal," +
                        "  weight_kg double" +
                        ")",

                "CREATE TYPE IF NOT EXISTS " + ks + "order_item (" +
                        "  product_id uuid," +
                        "  sku text," +
                        "  name text," +
                        "  quantity int," +
                        "  unit_price decimal" +
                        ")",

                "CREATE TYPE IF NOT EXISTS " + ks + "tracking_event (" +
                        "  status text," +
                        "  location text," +
                        "  occurred_at timestamp," +
                        "  note text" +
                        ")"
        );
    }

    private List<String> tables() {
        String ks = keyspace + ".";
        return List.of(
                // Customers: one partition per customer, no clustering. Rich personal
                // profile page with multiple addresses and lots of scalar types.
                "CREATE TABLE IF NOT EXISTS " + ks + "customers (" +
                        "  customer_id uuid PRIMARY KEY," +
                        "  email text," +
                        "  full_name text," +
                        "  nickname ascii," +
                        "  birth_date date," +
                        "  signup_time time," +
                        "  created_at timestamp," +
                        "  active boolean," +
                        "  loyalty_points int," +
                        "  lifetime_spend decimal," +
                        "  visits bigint," +
                        "  rating_avg float," +
                        "  trust_score double," +
                        "  age smallint," +
                        "  tier tinyint," +
                        "  external_ref varint," +
                        "  last_login_ip inet," +
                        "  avg_session duration," +
                        "  profile_blob blob," +
                        "  default_address frozen<address>," +
                        "  addresses list<frozen<address>>," +      // non-frozen list of frozen UDT
                        "  phones set<frozen<phone>>," +            // non-frozen set of frozen UDT
                        "  preferences map<text, text>," +          // non-frozen map
                        "  tags set<text>," +                       // non-frozen set
                        "  attributes frozen<map<text, text>>," +   // frozen map
                        "  recent_skus list<text>" +
                        ")",

                // Products: partitioned by category, clustered by product_id.
                "CREATE TABLE IF NOT EXISTS " + ks + "products (" +
                        "  category text," +
                        "  product_id uuid," +
                        "  sku text," +
                        "  name text," +
                        "  brand ascii," +
                        "  price decimal," +
                        "  stock int," +
                        "  reserved int," +
                        "  rating float," +
                        "  weight_kg double," +
                        "  release_date date," +
                        "  added_at timestamp," +
                        "  active boolean," +
                        "  dims frozen<dimensions>," +              // frozen UDT
                        "  attributes map<text, text>," +           // non-frozen map
                        "  features list<text>," +                  // non-frozen list
                        "  related_skus set<text>," +               // non-frozen set
                        "  specs frozen<map<text, text>>," +        // frozen map
                        "  warehouse_codes frozen<set<int>>," +     // frozen set
                        "  PRIMARY KEY ((category), product_id)" +
                        ") WITH CLUSTERING ORDER BY (product_id ASC)",

                // Orders: partitioned by customer, clustered by order_id (newest first).
                // Carries delivery tracking as a non-frozen list of frozen UDTs.
                "CREATE TABLE IF NOT EXISTS " + ks + "orders (" +
                        "  customer_id uuid," +
                        "  order_id timeuuid," +
                        "  status text," +
                        "  channel text," +
                        "  placed_at timestamp," +
                        "  updated_at timestamp," +
                        "  total decimal," +
                        "  currency ascii," +
                        "  item_count int," +
                        "  paid boolean," +
                        "  delivered boolean," +
                        "  shipping_address frozen<address>," +
                        "  items list<frozen<order_item>>," +       // non-frozen list of frozen UDT
                        "  tracking list<frozen<tracking_event>>," +// non-frozen list of frozen UDT
                        "  coupons set<text>," +
                        "  metadata map<text, text>," +
                        "  estimated_delivery date," +
                        "  PRIMARY KEY ((customer_id), order_id)" +
                        ") WITH CLUSTERING ORDER BY (order_id DESC)"
        );
    }
}
