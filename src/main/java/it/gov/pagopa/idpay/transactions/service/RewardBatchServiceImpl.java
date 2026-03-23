package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.TextStyle;
import java.util.Locale;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {

    private final RewardBatchRepository rewardBatchRepository;

    public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository) {
        this.rewardBatchRepository = rewardBatchRepository;
    }

    @Override
    public Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, String month, String businessName) {
        return rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(merchantId, posType,
                        month)
                .switchIfEmpty(Mono.defer(() ->
                        createBatch(merchantId, posType, month, businessName)
                                .doOnSuccess(batch -> log.info("[REWARD_BATCH_REPOSITORY]- findOrCreateBatch - created new batch with id: {}, month: {}",
                                        batch.getId(), batch.getMonth()))
                                .onErrorResume(DuplicateKeyException.class, ex ->
                                        rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(merchantId,
                                                posType, month))));
    }

    private Mono<RewardBatch> createBatch(String merchantId, PosType posType, String month, String businessName) {

        YearMonth batchYearMonth = YearMonth.parse(month);
        LocalDateTime startDate = batchYearMonth.atDay(1).atTime(0,0,0);
        LocalDateTime endDate = batchYearMonth.atEndOfMonth().atTime(23,59,59);

        RewardBatch batch = RewardBatch.builder()
                .merchantId(merchantId)
                .businessName(businessName)
                .month(month)
                .posType(posType)
                .status(RewardBatchStatus.CREATED)
                .partial(false)
                .name(buildBatchName(batchYearMonth))
                .startDate(startDate)
                .endDate(endDate)
                .approvedAmountCents(0L)
                .suspendedAmountCents(0L)
                .initialAmountCents(0L)
                .numberOfTransactions(0L)
                .numberOfTransactionsElaborated(0L)
                .reportPath(null)
                .assigneeLevel(RewardBatchAssignee.L1)
                .numberOfTransactionsSuspended(0L)
                .numberOfTransactionsRejected(0L)
                .creationDate(LocalDateTime.now())
                .updateDate(LocalDateTime.now())
                .build();

        return rewardBatchRepository.save(batch);
    }

    private String buildBatchName(YearMonth month) {
        String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
        String year = String.valueOf(month.getYear());

        return String.format("%s %s", monthName, year);
    }

}
