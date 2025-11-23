package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import org.springframework.dao.DuplicateKeyException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.TextStyle;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

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
                .onErrorResume(DuplicateKeyException.class, ex ->
                    rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(merchantId,
                        posType, month))));
  }

  @Override
  public Mono<Page<RewardBatch>> getMerchantRewardBatches(String merchantId, Pageable pageable) {
    return rewardBatchRepository.findRewardBatchByMerchantId(merchantId, pageable)
        .collectList()
        .zipWith(rewardBatchRepository.getCount(merchantId))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }

  @Override
  public Mono<Page<RewardBatch>> getAllRewardBatches(Pageable pageable) {
    return rewardBatchRepository.findRewardBatch(pageable)
        .collectList()
        .zipWith(rewardBatchRepository.getCount())
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
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
        .totalAmountCents(0L)
        .approvedAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .reportPath(null)
        .assigneLevel(RewardBatchAssignee.L1)
        .numberOfTransactionsSuspended(0L)
        .numberOfTransactionsRejected(0L)
        .build();

    return rewardBatchRepository.save(batch);
  }

  @Override
  public Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents) {
    return rewardBatchRepository.incrementTotals(batchId, accruedAmountCents);
  }

  private String buildBatchName(YearMonth month) {
    String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
    String year = String.valueOf(month.getYear());

    return String.format("%s %s", monthName, year);
  }
}
