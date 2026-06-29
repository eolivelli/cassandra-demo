package com.example.store.sim;

import com.datastax.oss.driver.api.core.data.CqlDuration;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random-but-plausible values for the store domain. All methods are
 * thread-safe (they rely on {@link ThreadLocalRandom}) so they can be called from
 * any worker thread.
 */
public final class DataFactory {

    private static final String[] FIRST = {
            "Ada", "Linus", "Grace", "Alan", "Margaret", "Dennis", "Barbara",
            "Ken", "Radia", "Donald", "Frances", "Tim", "Anita", "Guido"};
    private static final String[] LAST = {
            "Lovelace", "Torvalds", "Hopper", "Turing", "Hamilton", "Ritchie",
            "Liskov", "Thompson", "Perlman", "Knuth", "Allen", "Berners-Lee"};
    private static final String[] CITIES = {
            "Rome", "Milan", "Berlin", "Paris", "Madrid", "Lisbon", "Vienna",
            "Oslo", "Dublin", "Prague", "Warsaw", "Athens"};
    private static final String[] COUNTRIES = {"IT", "DE", "FR", "ES", "PT", "AT", "NO", "IE", "CZ", "PL", "GR"};
    private static final String[] STREETS = {"Main St", "Via Roma", "King Rd", "Park Ave", "Hill Ln", "Lake Blvd"};
    private static final String[] LABELS = {"home", "work", "billing", "shipping", "vacation"};
    private static final String[] PHONE_KINDS = {"mobile", "home", "work", "fax"};
    private static final String[] BRANDS = {
            "Dell", "HP", "Lenovo", "Asus", "Acer", "Logitech", "Razer", "Corsair",
            "Samsung", "LG", "MSI", "Gigabyte", "Kingston", "Seagate"};
    private static final String[] ORDER_STATUS = {"PLACED", "PACKING", "SHIPPED", "IN_TRANSIT", "DELIVERED"};
    private static final String[] TRACK_LOCATIONS = {"warehouse", "sorting-hub", "regional-depot", "local-courier", "doorstep"};
    private static final String[] CHANNELS = {"web", "mobile", "store", "partner"};
    private static final String[] CURRENCIES = {"EUR", "USD", "GBP"};

    private DataFactory() {
    }

    private static ThreadLocalRandom r() {
        return ThreadLocalRandom.current();
    }

    public static <T> T pick(T[] arr) {
        return arr[r().nextInt(arr.length)];
    }

    public static String email() {
        return (firstName() + "." + lastName() + r().nextInt(100000) + "@example.com").toLowerCase();
    }

    public static String firstName() {
        return pick(FIRST);
    }

    public static String lastName() {
        return pick(LAST);
    }

    public static String fullName() {
        return firstName() + " " + lastName();
    }

    public static String nickname() {
        return (firstName() + r().nextInt(1000)).toLowerCase();
    }

    public static String city() {
        return pick(CITIES);
    }

    public static String country() {
        return pick(COUNTRIES);
    }

    public static String street() {
        return (1 + r().nextInt(300)) + " " + pick(STREETS);
    }

    public static String addressLabel() {
        return pick(LABELS);
    }

    public static String phoneKind() {
        return pick(PHONE_KINDS);
    }

    public static String brand() {
        return pick(BRANDS);
    }

    public static String channel() {
        return pick(CHANNELS);
    }

    public static String currency() {
        return pick(CURRENCIES);
    }

    public static String orderStatus() {
        return pick(ORDER_STATUS);
    }

    public static String trackingLocation() {
        return pick(TRACK_LOCATIONS);
    }

    public static String zip() {
        return String.format("%05d", r().nextInt(100000));
    }

    public static String productName(String category) {
        return brand() + " " + category + " " + (char) ('A' + r().nextInt(26)) + (1000 + r().nextInt(9000));
    }

    public static String sku() {
        return "SKU-" + Integer.toHexString(r().nextInt(0x1000000)).toUpperCase();
    }

    public static int intRange(int lo, int hi) {
        return lo + r().nextInt(Math.max(1, hi - lo));
    }

    public static BigDecimal money(double lo, double hi) {
        double v = lo + r().nextDouble() * (hi - lo);
        return BigDecimal.valueOf(v).setScale(2, RoundingMode.HALF_UP);
    }

    public static BigDecimal dimension() {
        return BigDecimal.valueOf(1 + r().nextDouble() * 80).setScale(1, RoundingMode.HALF_UP);
    }

    public static double weightKg() {
        return Math.round((0.05 + r().nextDouble() * 15) * 100.0) / 100.0;
    }

    public static float rating() {
        return Math.round(r().nextDouble() * 50) / 10.0f;
    }

    public static double trustScore() {
        return Math.round(r().nextDouble() * 10000) / 100.0;
    }

    public static short smallint(int bound) {
        return (short) r().nextInt(bound);
    }

    public static byte tier() {
        return (byte) (1 + r().nextInt(5));
    }

    public static BigInteger varint() {
        return BigInteger.valueOf(r().nextLong(1_000_000_000_000L));
    }

    public static LocalDate pastDate(int maxYearsAgo) {
        return LocalDate.now().minusDays(r().nextInt(365 * maxYearsAgo + 1));
    }

    public static LocalDate futureDate(int maxDaysAhead) {
        return LocalDate.now().plusDays(1 + r().nextInt(maxDaysAhead));
    }

    public static LocalTime time() {
        return LocalTime.of(r().nextInt(24), r().nextInt(60), r().nextInt(60));
    }

    public static Instant now() {
        return Instant.now();
    }

    public static CqlDuration sessionDuration() {
        return CqlDuration.newInstance(0, 0, (r().nextInt(7200) + 30) * 1_000_000_000L);
    }

    public static InetAddress ip() {
        byte[] b = {(byte) r().nextInt(256), (byte) r().nextInt(256),
                (byte) r().nextInt(256), (byte) r().nextInt(256)};
        try {
            return Inet4Address.getByAddress(b);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    public static ByteBuffer blob(int maxLen) {
        byte[] b = new byte[1 + r().nextInt(maxLen)];
        r().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}
