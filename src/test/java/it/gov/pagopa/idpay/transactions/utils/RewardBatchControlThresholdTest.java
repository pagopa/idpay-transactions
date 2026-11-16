package it.gov.pagopa.idpay.transactions.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RewardBatchControlThresholdTest {

    @Test
    void calculatesCeilingForFifteenAndFiftyPercent() {
        assertEquals(2, RewardBatchControlThreshold.requiredTransactions(10, 15));
        assertEquals(5, RewardBatchControlThreshold.requiredTransactions(10, 50));
        assertEquals(1, RewardBatchControlThreshold.requiredTransactions(1, 15));
    }

    @Test
    void emptyBatchRequiresNoTransactions() {
        assertEquals(0, RewardBatchControlThreshold.requiredTransactions(0, 50));
        assertTrue(RewardBatchControlThreshold.isSatisfied(0, 0, 50));
    }

    @Test
    void rejectsInvalidPercentages() {
        assertThrows(IllegalArgumentException.class,
                () -> RewardBatchControlThreshold.requiredTransactions(10, 0));
        assertThrows(IllegalArgumentException.class,
                () -> RewardBatchControlThreshold.requiredTransactions(10, 101));
    }
}
