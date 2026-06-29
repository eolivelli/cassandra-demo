package com.example.store.sim;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.uuid.Uuids;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The heart of the simulation: a catalogue of "events" that, between them, exercise
 * standard mutations, lightweight transactions, logged and unlogged batches, reads,
 * partial updates and partial deletes against the store schema.
 *
 * <p>An instance owns the prepared statements and is fully thread-safe, so the same
 * instance is shared by every worker thread.
 */
public final class StoreOperations {

    // Operation names, also used as stat keys and weights for the random selector.
    public static final String CREATE_CUSTOMER = "create_customer";        // INSERT, LWT
    public static final String CREATE_PRODUCT = "create_product";          // INSERT
    public static final String UPDATE_CUSTOMER = "update_customer";        // partial UPDATE
    public static final String DELETE_CUSTOMER_PART = "delete_customer";   // partial DELETE
    public static final String PLACE_ORDER = "place_order";                // LOGGED BATCH + read
    public static final String UPDATE_TRACKING = "update_tracking";        // read + partial UPDATE
    public static final String CANCEL_ORDER = "cancel_order";              // LWT
    public static final String RESERVE_STOCK = "reserve_stock";            // read + LWT (compare-and-set)
    public static final String BULK_PRODUCT_UPDATE = "bulk_product_update";// UNLOGGED BATCH

    private final CqlSession session;
    private final SimulationConfig config;
    private final Stats stats;

    private final BoundedIdPool<UUID> customers;
    private final BoundedIdPool<ProductRef> products;

    // Weighted operation table.
    private final String[] ops = {
            CREATE_CUSTOMER, CREATE_PRODUCT, UPDATE_CUSTOMER, DELETE_CUSTOMER_PART,
            PLACE_ORDER, UPDATE_TRACKING, CANCEL_ORDER, RESERVE_STOCK, BULK_PRODUCT_UPDATE};
    private final int[] weights = {12, 8, 18, 8, 20, 12, 6, 8, 8};
    private final int[] cumulative;
    private final int totalWeight;

    // UDT type descriptors.
    private final UserDefinedType addressType;
    private final UserDefinedType phoneType;
    private final UserDefinedType dimensionsType;
    private final UserDefinedType orderItemType;
    private final UserDefinedType trackingEventType;

    // Prepared statements.
    private final PreparedStatement psInsertCustomer;
    private final PreparedStatement psInsertProduct;
    private final PreparedStatement psSetPreference;
    private final PreparedStatement psAddAddress;
    private final PreparedStatement psAddTags;
    private final PreparedStatement psAddPhone;
    private final PreparedStatement psSetScalars;
    private final PreparedStatement psSetDefaultAddress;
    private final PreparedStatement psDeletePreference;
    private final PreparedStatement psDeleteColumns;
    private final PreparedStatement psRemoveTag;
    private final PreparedStatement psReadProductsByCategory;
    private final PreparedStatement psReadCustomerTotals;
    private final PreparedStatement psInsertOrder;
    private final PreparedStatement psUpdateCustomerAfterOrder;
    private final PreparedStatement psReadRecentOrders;
    private final PreparedStatement psAppendTracking;
    private final PreparedStatement psCancelOrder;
    private final PreparedStatement psReadStock;
    private final PreparedStatement psCasStock;
    private final PreparedStatement psReadProductIds;
    private final PreparedStatement psBulkPriceUpdate;

    public StoreOperations(CqlSession session, SimulationConfig config, Stats stats) {
        this.session = session;
        this.config = config;
        this.stats = stats;
        this.customers = new BoundedIdPool<>(config.customerPoolSize);
        this.products = new BoundedIdPool<>(config.productPoolSize);

        this.cumulative = new int[weights.length];
        int acc = 0;
        for (int i = 0; i < weights.length; i++) {
            acc += weights[i];
            cumulative[i] = acc;
        }
        this.totalWeight = acc;

        String ks = config.keyspace;
        this.addressType = udt(ks, "address");
        this.phoneType = udt(ks, "phone");
        this.dimensionsType = udt(ks, "dimensions");
        this.orderItemType = udt(ks, "order_item");
        this.trackingEventType = udt(ks, "tracking_event");

        psInsertCustomer = session.prepare(
                "INSERT INTO customers (customer_id,email,full_name,nickname,birth_date,signup_time," +
                        "created_at,active,loyalty_points,lifetime_spend,visits,rating_avg,trust_score,age,tier," +
                        "external_ref,last_login_ip,avg_session,profile_blob,default_address,addresses,phones," +
                        "preferences,tags,attributes,recent_skus) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS");

        psInsertProduct = session.prepare(
                "INSERT INTO products (category,product_id,sku,name,brand,price,stock,reserved,rating,weight_kg," +
                        "release_date,added_at,active,dims,attributes,features,related_skus,specs,warehouse_codes) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

        psSetPreference = session.prepare(
                "UPDATE customers SET preferences[?]=? WHERE customer_id=?");
        psAddAddress = session.prepare(
                "UPDATE customers SET addresses = addresses + ? WHERE customer_id=?");
        psAddTags = session.prepare(
                "UPDATE customers SET tags = tags + ? WHERE customer_id=?");
        psAddPhone = session.prepare(
                "UPDATE customers SET phones = phones + ? WHERE customer_id=?");
        psSetScalars = session.prepare(
                "UPDATE customers SET loyalty_points=?, trust_score=?, rating_avg=? WHERE customer_id=?");
        psSetDefaultAddress = session.prepare(
                "UPDATE customers SET default_address=? WHERE customer_id=?");

        psDeletePreference = session.prepare(
                "DELETE preferences[?] FROM customers WHERE customer_id=?");
        psDeleteColumns = session.prepare(
                "DELETE avg_session, last_login_ip FROM customers WHERE customer_id=?");
        psRemoveTag = session.prepare(
                "UPDATE customers SET tags = tags - ? WHERE customer_id=?");

        psReadProductsByCategory = session.prepare(
                "SELECT product_id, sku, name, price FROM products WHERE category=? LIMIT ?");
        psReadCustomerTotals = session.prepare(
                "SELECT visits, lifetime_spend FROM customers WHERE customer_id=?");
        psInsertOrder = session.prepare(
                "INSERT INTO orders (customer_id,order_id,status,channel,placed_at,updated_at,total,currency," +
                        "item_count,paid,delivered,shipping_address,items,tracking,coupons,metadata,estimated_delivery) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        psUpdateCustomerAfterOrder = session.prepare(
                "UPDATE customers SET visits=?, lifetime_spend=?, recent_skus = recent_skus + ? WHERE customer_id=?");

        psReadRecentOrders = session.prepare(
                "SELECT order_id, delivered FROM orders WHERE customer_id=? LIMIT ?");
        psAppendTracking = session.prepare(
                "UPDATE orders SET tracking = tracking + ?, status=?, updated_at=?, delivered=? " +
                        "WHERE customer_id=? AND order_id=?");
        psCancelOrder = session.prepare(
                "UPDATE orders SET status=?, updated_at=? WHERE customer_id=? AND order_id=? IF delivered=?");

        psReadStock = session.prepare(
                "SELECT stock, reserved FROM products WHERE category=? AND product_id=?");
        psCasStock = session.prepare(
                "UPDATE products SET stock=?, reserved=? WHERE category=? AND product_id=? IF stock=?");

        psReadProductIds = session.prepare(
                "SELECT product_id FROM products WHERE category=? LIMIT ?");
        psBulkPriceUpdate = session.prepare(
                "UPDATE products SET price=?, attributes[?]=? WHERE category=? AND product_id=?");
    }

    private UserDefinedType udt(String ks, String name) {
        return session.getMetadata().getKeyspace(ks)
                .flatMap(k -> k.getUserDefinedType(name))
                .orElseThrow(() -> new IllegalStateException("UDT not found: " + name +
                        " (did you run 'create-schema' first?)"));
    }

    /** Picks a weighted-random event and executes it, recording stats and errors. */
    public void executeRandom() {
        String op = weightedPick();
        try {
            dispatch(op);
        } catch (RuntimeException e) {
            stats.recordError();
        }
    }

    private String weightedPick() {
        int x = ThreadLocalRandom.current().nextInt(totalWeight);
        for (int i = 0; i < cumulative.length; i++) {
            if (x < cumulative[i]) {
                return ops[i];
            }
        }
        return ops[ops.length - 1];
    }

    private void dispatch(String op) {
        switch (op) {
            case CREATE_CUSTOMER -> createCustomer();
            case CREATE_PRODUCT -> createProduct();
            case UPDATE_CUSTOMER -> updateCustomer();
            case DELETE_CUSTOMER_PART -> partialDeleteCustomer();
            case PLACE_ORDER -> placeOrder();
            case UPDATE_TRACKING -> updateTracking();
            case CANCEL_ORDER -> cancelOrder();
            case RESERVE_STOCK -> reserveStock();
            case BULK_PRODUCT_UPDATE -> bulkProductUpdate();
            default -> throw new IllegalStateException("unknown op " + op);
        }
    }

    // ---------------------------------------------------------------- creates

    private void createCustomer() {
        UUID id = UUID.randomUUID();
        List<UdtValue> addresses = new ArrayList<>();
        int n = DataFactory.intRange(1, 4);
        for (int i = 0; i < n; i++) {
            addresses.add(randomAddress());
        }
        Set<UdtValue> phones = new HashSet<>();
        int p = DataFactory.intRange(1, 3);
        for (int i = 0; i < p; i++) {
            phones.add(randomPhone());
        }
        Map<String, String> prefs = new HashMap<>();
        prefs.put("newsletter", String.valueOf(ThreadLocalRandom.current().nextBoolean()));
        prefs.put("theme", DataFactory.pick(new String[]{"light", "dark"}));
        Set<String> tags = new HashSet<>(List.of("new", DataFactory.pick(new String[]{"vip", "regular", "trial"})));
        Map<String, String> attributes = Map.of("source", DataFactory.channel(), "locale", DataFactory.country());

        BoundStatementBuilder b = psInsertCustomer.boundStatementBuilder()
                .setUuid("customer_id", id)
                .setString("email", DataFactory.email())
                .setString("full_name", DataFactory.fullName())
                .setString("nickname", DataFactory.nickname())
                .setLocalDate("birth_date", DataFactory.pastDate(60))
                .setLocalTime("signup_time", DataFactory.time())
                .setInstant("created_at", DataFactory.now())
                .setBoolean("active", true)
                .setInt("loyalty_points", DataFactory.intRange(0, 500))
                .setBigDecimal("lifetime_spend", DataFactory.money(0, 1000))
                .setLong("visits", DataFactory.intRange(0, 50))
                .setFloat("rating_avg", DataFactory.rating())
                .setDouble("trust_score", DataFactory.trustScore())
                .setShort("age", DataFactory.smallint(90))
                .setByte("tier", DataFactory.tier())
                .setBigInteger("external_ref", DataFactory.varint())
                .setInetAddress("last_login_ip", DataFactory.ip())
                .setCqlDuration("avg_session", DataFactory.sessionDuration())
                .setByteBuffer("profile_blob", DataFactory.blob(64))
                .setUdtValue("default_address", addresses.get(0))
                .setList("addresses", addresses, UdtValue.class)
                .setSet("phones", phones, UdtValue.class)
                .setMap("preferences", prefs, String.class, String.class)
                .setSet("tags", tags, String.class)
                .setMap("attributes", attributes, String.class, String.class)
                .setList("recent_skus", List.of(), String.class);

        ResultSet rs = session.execute(b.build());
        stats.recordLwt(rs.wasApplied());
        customers.add(id);
        stats.record(CREATE_CUSTOMER);
    }

    private void createProduct() {
        String category = DataFactory.pick(config.categories.toArray(new String[0]));
        UUID id = UUID.randomUUID();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("color", DataFactory.pick(new String[]{"black", "silver", "white"}));
        attributes.put("warranty", DataFactory.intRange(12, 36) + "m");
        Map<String, String> specs = Map.of(
                "connector", DataFactory.pick(new String[]{"usb-c", "hdmi", "displayport", "pcie"}),
                "origin", DataFactory.country());
        Set<Integer> warehouses = new HashSet<>();
        int w = DataFactory.intRange(1, 4);
        for (int i = 0; i < w; i++) {
            warehouses.add(DataFactory.intRange(1, 20));
        }

        BoundStatementBuilder b = psInsertProduct.boundStatementBuilder()
                .setString("category", category)
                .setUuid("product_id", id)
                .setString("sku", DataFactory.sku())
                .setString("name", DataFactory.productName(category))
                .setString("brand", DataFactory.brand())
                .setBigDecimal("price", DataFactory.money(20, 3000))
                .setInt("stock", DataFactory.intRange(0, 1000))
                .setInt("reserved", 0)
                .setFloat("rating", DataFactory.rating())
                .setDouble("weight_kg", DataFactory.weightKg())
                .setLocalDate("release_date", DataFactory.pastDate(3))
                .setInstant("added_at", DataFactory.now())
                .setBoolean("active", true)
                .setUdtValue("dims", randomDimensions())
                .setMap("attributes", attributes, String.class, String.class)
                .setList("features", List.of("rgb", "wireless", "ergonomic"), String.class)
                .setSet("related_skus", Set.of(DataFactory.sku(), DataFactory.sku()), String.class)
                .setMap("specs", specs, String.class, String.class)
                .setSet("warehouse_codes", warehouses, Integer.class);

        session.execute(b.build());
        products.add(new ProductRef(category, id));
        stats.record(CREATE_PRODUCT);
    }

    // -------------------------------------------------------- partial updates

    private void updateCustomer() {
        UUID id = customers.sample();
        if (id == null) {
            createCustomer();
            return;
        }
        switch (ThreadLocalRandom.current().nextInt(6)) {
            case 0 -> session.execute(psSetPreference.boundStatementBuilder()
                    .setString(0, "pref" + DataFactory.intRange(0, 5))
                    .setString(1, DataFactory.pick(new String[]{"yes", "no", "maybe"}))
                    .setUuid(2, id).build());
            case 1 -> session.execute(psAddAddress.boundStatementBuilder()
                    .setList(0, List.of(randomAddress()), UdtValue.class)
                    .setUuid(1, id).build());
            case 2 -> session.execute(psAddTags.boundStatementBuilder()
                    .setSet(0, Set.of(DataFactory.pick(new String[]{"promo", "beta", "wholesale"})), String.class)
                    .setUuid(1, id).build());
            case 3 -> session.execute(psAddPhone.boundStatementBuilder()
                    .setSet(0, Set.of(randomPhone()), UdtValue.class)
                    .setUuid(1, id).build());
            case 4 -> session.execute(psSetScalars.boundStatementBuilder()
                    .setInt(0, DataFactory.intRange(0, 5000))
                    .setDouble(1, DataFactory.trustScore())
                    .setFloat(2, DataFactory.rating())
                    .setUuid(3, id).build());
            default -> session.execute(psSetDefaultAddress.boundStatementBuilder()
                    .setUdtValue(0, randomAddress())
                    .setUuid(1, id).build());
        }
        stats.record(UPDATE_CUSTOMER);
    }

    private void partialDeleteCustomer() {
        UUID id = customers.sample();
        if (id == null) {
            createCustomer();
            return;
        }
        switch (ThreadLocalRandom.current().nextInt(3)) {
            case 0 -> session.execute(psDeletePreference.boundStatementBuilder()
                    .setString(0, "pref" + DataFactory.intRange(0, 5))
                    .setUuid(1, id).build());
            case 1 -> session.execute(psDeleteColumns.boundStatementBuilder()
                    .setUuid(0, id).build());
            default -> session.execute(psRemoveTag.boundStatementBuilder()
                    .setSet(0, Set.of("new"), String.class)
                    .setUuid(1, id).build());
        }
        stats.record(DELETE_CUSTOMER_PART);
    }

    // ------------------------------------------------------------- orders

    private void placeOrder() {
        UUID customerId = customers.sample();
        if (customerId == null) {
            createCustomer();
            return;
        }
        String category = DataFactory.pick(config.categories.toArray(new String[0]));

        // Read a handful of products to put in the basket.
        ResultSet productRs = session.execute(psReadProductsByCategory.boundStatementBuilder()
                .setString(0, category).setInt(1, 10).build());
        stats.recordRead();
        List<Row> productRows = productRs.all();
        if (productRows.isEmpty()) {
            createProduct();
            return;
        }

        int lines = Math.min(productRows.size(), DataFactory.intRange(1, 4));
        List<UdtValue> items = new ArrayList<>();
        BigDecimal total = BigDecimal.ZERO;
        for (int i = 0; i < lines; i++) {
            Row pr = productRows.get(ThreadLocalRandom.current().nextInt(productRows.size()));
            int qty = DataFactory.intRange(1, 4);
            BigDecimal unit = pr.getBigDecimal("price");
            if (unit == null) {
                unit = DataFactory.money(20, 500);
            }
            total = total.add(unit.multiply(BigDecimal.valueOf(qty)));
            items.add(orderItemType.newValue()
                    .setUuid("product_id", pr.getUuid("product_id"))
                    .setString("sku", pr.getString("sku"))
                    .setString("name", pr.getString("name"))
                    .setInt("quantity", qty)
                    .setBigDecimal("unit_price", unit));
        }

        UUID orderId = Uuids.timeBased();
        var insert = psInsertOrder.boundStatementBuilder()
                .setUuid("customer_id", customerId)
                .setUuid("order_id", orderId)
                .setString("status", "PLACED")
                .setString("channel", DataFactory.channel())
                .setInstant("placed_at", DataFactory.now())
                .setInstant("updated_at", DataFactory.now())
                .setBigDecimal("total", total)
                .setString("currency", DataFactory.currency())
                .setInt("item_count", lines)
                .setBoolean("paid", ThreadLocalRandom.current().nextBoolean())
                .setBoolean("delivered", false)
                .setUdtValue("shipping_address", randomAddress())
                .setList("items", items, UdtValue.class)
                .setList("tracking", List.of(randomTracking("PLACED")), UdtValue.class)
                .setSet("coupons", Set.of("WELCOME10"), String.class)
                .setMap("metadata", Map.of("device", DataFactory.channel()), String.class, String.class)
                .setLocalDate("estimated_delivery", DataFactory.futureDate(14))
                .build();

        // Read the customer's running totals so we can write back consistent values
        // (no counters allowed, so this is a read-modify-write).
        Row totals = session.execute(psReadCustomerTotals.boundStatementBuilder()
                .setUuid(0, customerId).build()).one();
        stats.recordRead();

        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED).addStatement(insert);
        if (totals != null) {
            long visits = totals.getLong("visits") + 1;
            BigDecimal spend = totals.getBigDecimal("lifetime_spend");
            spend = (spend == null ? total : spend.add(total));
            List<String> skus = new ArrayList<>();
            for (UdtValue item : items) {
                skus.add(item.getString("sku"));
            }
            batch.addStatement(psUpdateCustomerAfterOrder.boundStatementBuilder()
                    .setLong(0, visits)
                    .setBigDecimal(1, spend)
                    .setList(2, skus, String.class)
                    .setUuid(3, customerId).build());
        }
        session.execute(batch.build());
        stats.record(PLACE_ORDER);
    }

    private void updateTracking() {
        UUID customerId = customers.sample();
        if (customerId == null) {
            createCustomer();
            return;
        }
        Row order = pickRecentOrder(customerId);
        if (order == null) {
            placeOrder();
            return;
        }
        String status = DataFactory.orderStatus();
        boolean delivered = "DELIVERED".equals(status);
        session.execute(psAppendTracking.boundStatementBuilder()
                .setList(0, List.of(randomTracking(status)), UdtValue.class)
                .setString(1, status)
                .setInstant(2, DataFactory.now())
                .setBoolean(3, delivered)
                .setUuid(4, customerId)
                .setUuid(5, order.getUuid("order_id"))
                .build());
        stats.record(UPDATE_TRACKING);
    }

    private void cancelOrder() {
        UUID customerId = customers.sample();
        if (customerId == null) {
            createCustomer();
            return;
        }
        Row order = pickRecentOrder(customerId);
        if (order == null) {
            placeOrder();
            return;
        }
        ResultSet rs = session.execute(psCancelOrder.boundStatementBuilder()
                .setString(0, "CANCELLED")
                .setInstant(1, DataFactory.now())
                .setUuid(2, customerId)
                .setUuid(3, order.getUuid("order_id"))
                .setBoolean(4, false) // IF delivered = false
                .build());
        stats.recordLwt(rs.wasApplied());
        stats.record(CANCEL_ORDER);
    }

    private Row pickRecentOrder(UUID customerId) {
        ResultSet rs = session.execute(psReadRecentOrders.boundStatementBuilder()
                .setUuid(0, customerId).setInt(1, 10).build());
        stats.recordRead();
        List<Row> rows = rs.all();
        if (rows.isEmpty()) {
            return null;
        }
        return rows.get(ThreadLocalRandom.current().nextInt(rows.size()));
    }

    // ------------------------------------------------------------- inventory

    private void reserveStock() {
        ProductRef ref = products.sample();
        if (ref == null) {
            createProduct();
            return;
        }
        Row row = session.execute(psReadStock.boundStatementBuilder()
                .setString(0, ref.category()).setUuid(1, ref.productId()).build()).one();
        stats.recordRead();
        if (row != null) {
            int stock = row.getInt("stock");
            int reserved = row.getInt("reserved");
            int qty = DataFactory.intRange(1, 5);
            if (stock >= qty) {
                // Compare-and-set: only commit if nobody changed stock in the meantime.
                ResultSet rs = session.execute(psCasStock.boundStatementBuilder()
                        .setInt(0, stock - qty)
                        .setInt(1, reserved + qty)
                        .setString(2, ref.category())
                        .setUuid(3, ref.productId())
                        .setInt(4, stock)
                        .build());
                stats.recordLwt(rs.wasApplied());
            }
        }
        stats.record(RESERVE_STOCK);
    }

    private void bulkProductUpdate() {
        String category = DataFactory.pick(config.categories.toArray(new String[0]));
        ResultSet rs = session.execute(psReadProductIds.boundStatementBuilder()
                .setString(0, category).setInt(1, 25).build());
        stats.recordRead();
        List<Row> rows = rs.all();
        if (rows.isEmpty()) {
            createProduct();
            return;
        }
        // All rows share the same partition (category), so an UNLOGGED batch is the
        // efficient choice here.
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        int n = Math.min(rows.size(), DataFactory.intRange(2, 10));
        for (int i = 0; i < n; i++) {
            Row pr = rows.get(i);
            batch.addStatement(psBulkPriceUpdate.boundStatementBuilder()
                    .setBigDecimal(0, DataFactory.money(20, 3000))
                    .setString(1, "promo")
                    .setString(2, "-" + DataFactory.intRange(5, 40) + "%")
                    .setString(3, category)
                    .setUuid(4, pr.getUuid("product_id"))
                    .build());
        }
        session.execute(batch.build());
        stats.record(BULK_PRODUCT_UPDATE);
    }

    // --------------------------------------------------------- UDT builders

    private UdtValue randomAddress() {
        return addressType.newValue()
                .setString("label", DataFactory.addressLabel())
                .setString("street", DataFactory.street())
                .setString("city", DataFactory.city())
                .setString("state", DataFactory.city())
                .setString("zip", DataFactory.zip())
                .setString("country", DataFactory.country())
                .setDouble("lat", ThreadLocalRandom.current().nextDouble(-90, 90))
                .setDouble("lon", ThreadLocalRandom.current().nextDouble(-180, 180));
    }

    private UdtValue randomPhone() {
        return phoneType.newValue()
                .setShort("country_code", (short) DataFactory.intRange(1, 99))
                .setString("number", String.format("%010d", DataFactory.intRange(0, 1_000_000_000)))
                .setString("kind", DataFactory.phoneKind())
                .setBoolean("verified", ThreadLocalRandom.current().nextBoolean());
    }

    private UdtValue randomDimensions() {
        return dimensionsType.newValue()
                .setBigDecimal("width_cm", DataFactory.dimension())
                .setBigDecimal("height_cm", DataFactory.dimension())
                .setBigDecimal("depth_cm", DataFactory.dimension())
                .setDouble("weight_kg", DataFactory.weightKg());
    }

    private UdtValue randomTracking(String status) {
        return trackingEventType.newValue()
                .setString("status", status)
                .setString("location", DataFactory.trackingLocation())
                .setInstant("occurred_at", DataFactory.now())
                .setString("note", "auto-generated");
    }
}
