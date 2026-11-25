package it.gov.pagopa.idpay.transactions.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class RewardBatchIdGeneratorTest {

    private static final Pattern OBJECT_ID_PATTERN = Pattern.compile("^[0-9a-f]{24}$");

    @RepeatedTest(20)
    void shouldGenerateTwentyFourLowercaseHexadecimalCharacters() {
        String id = RewardBatchIdGenerator.generate();

        assertEquals(24, id.length());
        assertTrue(OBJECT_ID_PATTERN.matcher(id).matches(), "id '%s' does not match ^[0-9a-f]{24}$".formatted(id));
    }

    @Test
    void shouldGenerateUniqueIdsAcrossManySequentialCalls() {
        int total = 1_000_000;
        Set<String> ids = ConcurrentHashMap.newKeySet(total);

        for (int i = 0; i < total; i++) {
            ids.add(RewardBatchIdGenerator.generate());
        }

        assertEquals(total, ids.size());
    }

    @Test
    void shouldGenerateUniqueIdsUnderConcurrentAccess() throws InterruptedException {
        int threads = 16;
        int perThread = 20_000;
        int total = threads * perThread;

        Set<String> ids = ConcurrentHashMap.newKeySet(total);
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(threads);
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        try {
            for (int t = 0; t < threads; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < perThread; i++) {
                            if (!ids.add(RewardBatchIdGenerator.generate())) {
                                failures.incrementAndGet();
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(60, TimeUnit.SECONDS), "generation did not complete in time");
        } finally {
            executor.shutdownNow();
        }

        assertEquals(0, failures.get());
        assertEquals(total, ids.size());
    }

    @Test
    void shouldGenerateIdsWithNonDecreasingTimestampComponent() throws InterruptedException {
        String first = RewardBatchIdGenerator.generate();
        Thread.sleep(1100);
        String second = RewardBatchIdGenerator.generate();

        long firstTimestamp = Long.parseLong(first.substring(0, 8), 16);
        long secondTimestamp = Long.parseLong(second.substring(0, 8), 16);

        assertTrue(secondTimestamp >= firstTimestamp);
    }
}
