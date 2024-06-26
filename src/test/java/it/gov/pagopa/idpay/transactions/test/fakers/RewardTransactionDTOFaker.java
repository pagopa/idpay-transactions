package it.gov.pagopa.idpay.transactions.test.fakers;

import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.TransactionProcessed;
import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;

import java.time.*;
import java.util.*;

public class RewardTransactionDTOFaker {
    private RewardTransactionDTOFaker() {
    }

    private static final Random randomGenerator = new Random();

    private static Random getRandom(Integer bias) {
        return bias == null ? randomGenerator : new Random(bias);
    }

    private static int getRandomPositiveNumber(Integer bias) {
        return Math.abs(getRandom(bias).nextInt());
    }

    private static int getRandomPositiveNumber(Integer bias, int bound) {
        return Math.abs(getRandom(bias).nextInt(bound));
    }

    private static final FakeValuesService fakeValuesServiceGlobal = new FakeValuesService(new Locale("it"), new RandomService(null));

    private static FakeValuesService getFakeValuesService(Integer bias) {
        return bias == null ? fakeValuesServiceGlobal : new FakeValuesService(new Locale("it"), new RandomService(getRandom(bias)));
    }

    /**
     * It will return an example of {@link RewardTransactionDTO}. Providing a bias, it will return a pseudo-casual object
     */
    public static RewardTransactionDTO mockInstance(Integer bias) {
        return mockInstanceBuilder(bias).build();
    }

    public static RewardTransactionDTO.RewardTransactionDTOBuilder mockInstanceBuilder(Integer bias) {
        LocalDate trxDate = LocalDate.of(2022, getRandomPositiveNumber(bias, 11) + 1, getRandomPositiveNumber(bias, 27)+1);
        LocalTime trxTime = LocalTime.of(getRandomPositiveNumber(bias, 23), getRandomPositiveNumber(bias, 59), getRandomPositiveNumber(bias, 59));
        LocalDateTime trxDateTime = LocalDateTime.of(trxDate, trxTime);

        RewardTransactionDTO.RewardTransactionDTOBuilder out = RewardTransactionDTO.builder();

        out.idTrxAcquirer("IDTRXACQUIRER%d".formatted(bias));
        out.acquirerCode("ACQUIRERCODE%d".formatted(bias));
        out.trxDate(OffsetDateTime.of(
                trxDateTime,
                ZoneId.of("Europe/Rome").getRules().getOffset(trxDateTime)));
        out.hpan("HPAN%s".formatted(bias));
        out.operationType("OPERATIONTYPE%d".formatted(bias));
        out.circuitType("CIRCUITTYPE%d".formatted(bias));
        out.idTrxIssuer("IDTRXISSUER%d".formatted(bias));
        out.correlationId("CORRELATIONID%d".formatted(bias));
        out.amountCents(getRandomPositiveNumber(bias, 200)*100L);
        out.amountCurrency("AMOUNTCURRENCY%d".formatted(bias));
        out.mcc("MCC%d".formatted(bias));
        out.acquirerId("ACQUIRERID%d".formatted(bias));
        out.merchantId("MERCHANTID%d".formatted(bias));
        out.terminalId("TERMINALID%d".formatted(bias));
        out.bin("BIN%d".formatted(bias));
        out.senderCode("SENDERCODE%d".formatted(bias));
        out.fiscalCode("FISCALCODE%d".formatted(bias));
        out.vat("VAT%d".formatted(bias));
        out.posType("POSTYPE%d".formatted(bias));
        out.par("PAR%d".formatted(bias));
        out.status("STATUS%d".formatted(bias));
        out.userId("USERID%d".formatted(bias));
        out.maskedPan("MASKEDPAN%d".formatted(bias));
        out.brandLogo("BRANDLOGO%d".formatted(bias));
        out.channel("CHANNEL%d".formatted(bias));
        return out;
    }

    public static RewardTransactionDTO mockInstanceRefund(Integer bias){
        RewardTransactionDTO out = mockInstanceBuilder(bias).build();
        out.setOperationTypeTranscoded("OperationTypeTranscoded");
        out.setEffectiveAmountCents(1000L);
        out.setTrxChargeDate(out.getTrxDate().minusDays(1));
        out.setStatus("STATUS%d".formatted(bias));
        String initiativeId = "INITIATIVEID%d".formatted(bias);
        out.setInitiatives(List.of(initiativeId));
        out.setElaborationDateTime(LocalDateTime.now());

        Map<String, Reward> reward = new HashMap<>();

        RewardCounters counter = RewardCounters.builder()
                .exhaustedBudget(false)
                .initiativeBudgetCents(10000L)
                .build();

        Reward rewardElement = Reward.builder()
                .initiativeId(initiativeId)
                .organizationId("ORGANIZATIONID")
                .providedRewardCents(1000L)
                .accruedRewardCents(1000L)
                .capped(false)
                .dailyCapped(false)
                .monthlyCapped(false)
                .yearlyCapped(false)
                .weeklyCapped(false)
                .counters(counter)
                .build();
        reward.put(initiativeId,rewardElement);
        out.setRewards(reward);
        out.setTrxChargeDate(OffsetDateTime.now());

        TransactionProcessed transactionProcessed = TransactionProcessed.builder()
                .id(out.getId())
                .idTrxAcquirer(out.getIdTrxAcquirer())
                .acquirerCode(out.getAcquirerCode())
                .trxDate(out.getTrxDate().toLocalDateTime())
                .operationType(out.getOperationType())
                .acquirerId(out.getAcquirerId())
                .userId(out.getUserId())
                .correlationId(out.getCorrelationId())
                .amountCents(1000L)
                .amountCents(1000L)
                .rewards(out.getRewards())
                .effectiveAmountCents(1000L)
                .trxChargeDate(out.getTrxChargeDate().toLocalDateTime())
                .operationTypeTranscoded(out.getOperationTypeTranscoded())
                .timestamp(LocalDateTime.now())
                .build();
        HashMap<String, RefundInfo.PreviousReward> previousRewards = new HashMap<>();
        previousRewards.put("initiativeID", new RefundInfo.PreviousReward("initiativeID", "organizationID", 1000L));
        RefundInfo refundInfo = RefundInfo.builder()
                .previousTrxs(List.of(transactionProcessed))
                .previousRewards(previousRewards)
                .build();
        out.setRefundInfo(refundInfo);
        return out;
    }

    public static RewardTransactionDTO mockInstanceRejected(Integer bias){
        RewardTransactionDTO out = mockInstanceBuilder(bias).build();
        out.setStatus("REJECTED");
        out.setRejectionReasons(List.of("ERROR"));
        out.setTrxChargeDate(OffsetDateTime.now());

        Map<String, List<String>> initiativeRejectionsReason = new HashMap<>();
        initiativeRejectionsReason.put("initiative", List.of("Error initiative"));
        out.setInitiativeRejectionReasons(initiativeRejectionsReason);
        out.setElaborationDateTime(LocalDateTime.now());

        return out;
    }
}
