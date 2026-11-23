package it.gov.pagopa.idpay.transactions.model;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates reward batch identifiers compatible with the 24 lowercase hexadecimal
 * characters format of MongoDB's ObjectId ({@code ^[0-9a-f]{24}$}), matching the
 * identifiers already in production for batches created on Mongo.
 *
 * <p>The identifier is built, like an ObjectId, from 12 bytes: a 4-byte epoch-second
 * timestamp, a 5-byte value random per JVM instance, and a 3-byte counter shared across
 * threads. This guarantees uniqueness without requiring the {@code org.bson} dependency,
 * which the project does not otherwise need.</p>
 */
public final class RewardBatchIdGenerator {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final byte[] PROCESS_UNIQUE = new byte[5];
    private static final AtomicInteger COUNTER = new AtomicInteger(SECURE_RANDOM.nextInt());

    static {
        SECURE_RANDOM.nextBytes(PROCESS_UNIQUE);
    }

    private RewardBatchIdGenerator() {
    }

    public static String generate() {
        byte[] id = new byte[12];

        int timestamp = (int) (System.currentTimeMillis() / 1000L);
        id[0] = (byte) (timestamp >>> 24);
        id[1] = (byte) (timestamp >>> 16);
        id[2] = (byte) (timestamp >>> 8);
        id[3] = (byte) timestamp;

        System.arraycopy(PROCESS_UNIQUE, 0, id, 4, 5);

        int counter = COUNTER.getAndIncrement() & 0xFFFFFF;
        id[9] = (byte) (counter >>> 16);
        id[10] = (byte) (counter >>> 8);
        id[11] = (byte) counter;

        return toHex(id);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16));
            hex.append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }
}
