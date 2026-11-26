package it.gov.pagopa.idpay.transactions.utils;

public final class RewardBatchControlThreshold {

    private RewardBatchControlThreshold() {
    }

    public static int validate(int percentage) {
        if (percentage < 1 || percentage > 100) {
            throw new IllegalArgumentException(
                    "Reward batch control threshold must be between 1 and 100"
            );
        }
        return percentage;
    }

    public static long requiredTransactions(long totalTransactions, int percentage) {
        if (totalTransactions < 0) {
            throw new IllegalArgumentException("Total transactions cannot be negative");
        }
        validate(percentage);
        return Math.ceilDiv(Math.multiplyExact(totalTransactions, percentage), 100L);
    }

    public static boolean isSatisfied(long totalTransactions, long elaboratedTransactions, int percentage) {
        return totalTransactions >= 0
                && elaboratedTransactions >= requiredTransactions(totalTransactions, percentage);
    }
}
