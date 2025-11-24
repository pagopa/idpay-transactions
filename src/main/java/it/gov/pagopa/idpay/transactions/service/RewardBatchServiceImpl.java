package it.gov.pagopa.idpay.transactions.service;

import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import lombok.Data;
import org.springframework.dao.DuplicateKeyException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.TextStyle;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {

  private final RewardBatchRepository rewardBatchRepository;
  private final ReactiveMongoTemplate reactiveMongoTemplate;

  public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository, ReactiveMongoTemplate reactiveMongoTemplate) {
    this.rewardBatchRepository = rewardBatchRepository;
    this.reactiveMongoTemplate = reactiveMongoTemplate;
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
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactionsSuspended(0L)
        .numberOfTransactionsRejected(0L)
        .build();

    return rewardBatchRepository.save(batch);
  }

  @Override
  public Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents) {
    return rewardBatchRepository.incrementTotals(batchId, accruedAmountCents);
  }

    @Override
    public Mono<RewardBatch> suspendTransactions(String rewardBatchId, TransactionsRequest request) {
        return rewardBatchRepository.findById(rewardBatchId)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Batch not found: " + rewardBatchId)))
                .flatMap(batch -> {

                    if (RewardBatchStatus.APPROVED.equals(batch.getStatus())) {
                        log.info("Batch {} APPROVED, skipping suspension", rewardBatchId);
                        return Mono.error(new IllegalStateException("Cannot suspend transactions on an APPROVED batch"));
                    }

                    return updateTransactionsStatus(
                            rewardBatchId,
                            request.getTransactionIds(),
                            RewardBatchTrxStatus.SUSPENDED,
                            request.getReason()
                    ).flatMap(modifiedCount -> {
                        //da modificare, se 0 c'è stato un errore
                        if (modifiedCount == 0) {
                            return Mono.just(batch);
                        }

                        MatchOperation match = Aggregation.match(
                                Criteria.where("rewardBatchId").is(rewardBatchId)
                                        .and("id").in(request.getTransactionIds())
                                        .and("rewardBatchTrxStatus").is(RewardBatchTrxStatus.SUSPENDED)
                        );

                        Aggregation agg = Aggregation.newAggregation(
                                match,
                                //dubbio se sia accruedRewardCents
                                Aggregation.group().sum("amountCents").as("total")
                        );

                        return Mono.from(reactiveMongoTemplate.aggregate(agg, RewardTransaction.class, TotalAmount.class)
                                        .next())
                                .defaultIfEmpty(new TotalAmount())
                                .flatMap(totalAmount -> {
                                    long suspendedTotal = totalAmount.getTotal();

                                    Update update = new Update()
                                            .inc("numberOfTransactionsElaborated", modifiedCount)
                                            .inc("approvedAmountCents", -suspendedTotal)
                                            .inc("numberOfTransactionsSuspended", modifiedCount)
                                            .currentDate("updateDate");

                                    Query query = Query.query(Criteria.where("_id").is(rewardBatchId));
                                    return reactiveMongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), RewardBatch.class);
                                });
                    });
                });
    }

    @Override
    public Mono<Long> updateTransactionsStatus(String rewardBatchId, List<String> transactionIds, RewardBatchTrxStatus newStatus, String reason) {
        Query query = new Query(
                Criteria.where("rewardBatchId").is(rewardBatchId)
                        .and("id").in(transactionIds));

        Update update = new Update()
                .set("rewardBatchTrxStatus", newStatus)
                .set("rewardBatchRejectionReason", reason);

        return reactiveMongoTemplate.updateMulti(query, update, RewardTransaction.class)
                .map(UpdateResult::getModifiedCount);
    }

    private String buildBatchName(YearMonth month) {
    String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
    String year = String.valueOf(month.getYear());

    return String.format("%s %s", monthName, year);
  }

    @Data
    static class TotalAmount {
        private long total;
    }
}
