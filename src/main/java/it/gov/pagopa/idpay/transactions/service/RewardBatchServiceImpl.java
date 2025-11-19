package it.gov.pagopa.idpay.transactions.service;

import org.springframework.dao.DuplicateKeyException;
import it.gov.pagopa.idpay.transactions.enums.BatchType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
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
  public Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, String month,
      BatchType batchType) {
    return rewardBatchRepository.findByMerchantIdAndPosTypeAndMonthAndBatchType(merchantId, posType,
            month, batchType)
        .switchIfEmpty(Mono.defer(() ->
            createBatch(merchantId, posType, month, batchType)
                .onErrorResume(DuplicateKeyException.class, ex ->
                    rewardBatchRepository.findByMerchantIdAndPosTypeAndMonthAndBatchType(merchantId,
                        posType, month, batchType))));
  }

  @Override
  public Mono<Page<RewardBatch>> getMerchantRewardBatches(String merchantId, Pageable pageable) {
    return rewardBatchRepository.findRewardBatchByMerchantId(merchantId, pageable)
        .collectList()
        .zipWith(rewardBatchRepository.getCount(merchantId))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }

  private Mono<RewardBatch> createBatch(String merchantId, PosType posType, String month, BatchType batchType) {

    YearMonth batchYearMonth = YearMonth.parse(month);
    LocalDateTime startDate = batchYearMonth.atDay(1).atStartOfDay();
    LocalDateTime endDate = batchYearMonth.atEndOfMonth().atTime(23,59,59);

    RewardBatch batch = RewardBatch.builder()
        .merchantId(merchantId)
        .month(month)
        .posType(posType)
        .batchType(batchType)
        .status(RewardBatchStatus.CREATED)
        .partial(false)
        .name(buildBatchName(batchYearMonth, posType, batchType))
        .startDate(startDate)
        .endDate(endDate)
        .build();

    return rewardBatchRepository.save(batch);
  }

  private String buildBatchName(YearMonth month, PosType posType, BatchType batchType) {
    String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
    String year = String.valueOf(month.getYear());
    String posLabel = posType == PosType.PHYSICAL ? "fisico" : "online";

    return batchType == BatchType.REJECTED ?
            String.format("%s %s - %s - rigettati", monthName, year, posLabel) :
            String.format("%s %s - %s", monthName, year, posLabel);
  }
}
