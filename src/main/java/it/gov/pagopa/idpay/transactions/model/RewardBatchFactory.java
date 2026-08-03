package it.gov.pagopa.idpay.transactions.model;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.TextStyle;
import java.util.Locale;

public final class RewardBatchFactory {

    private RewardBatchFactory() {
    }

    public static RewardBatch create(
            String initiativeId,
            String merchantId,
            PosType posType,
            String month,
            String businessName
    ) {
        YearMonth batchYearMonth = YearMonth.parse(month);
        LocalDateTime now = LocalDateTime.now(ZONEID);

        return RewardBatch.builder()
                .merchantId(merchantId)
                .initiativeId(initiativeId)
                .businessName(businessName)
                .month(month)
                .posType(posType)
                .status(RewardBatchStatus.CREATED)
                .partial(false)
                .name("%s %s".formatted(
                        batchYearMonth.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN),
                        batchYearMonth.getYear()
                ))
                .startDate(batchYearMonth.atDay(1).atStartOfDay())
                .endDate(batchYearMonth.atEndOfMonth().atTime(23, 59, 59))
                .approvedAmountCents(0L)
                .suspendedAmountCents(0L)
                .initialAmountCents(0L)
                .numberOfTransactions(0L)
                .numberOfTransactionsElaborated(0L)
                .assigneeLevel(RewardBatchAssignee.L1)
                .numberOfTransactionsSuspended(0L)
                .numberOfTransactionsRejected(0L)
                .creationDate(now)
                .updateDate(now)
                .build();
    }
}
