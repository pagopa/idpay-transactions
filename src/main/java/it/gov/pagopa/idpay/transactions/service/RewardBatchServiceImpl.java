package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import lombok.Data;
import org.springframework.dao.DuplicateKeyException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
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
  private final RewardTransactionRepository rewardTransactionRepository;
  private final ReactiveMongoTemplate reactiveMongoTemplate;

  public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository,
                                RewardTransactionRepository rewardTransactionRepository,
                                ReactiveMongoTemplate reactiveMongoTemplate) {
    this.rewardBatchRepository = rewardBatchRepository;
    this.rewardTransactionRepository = rewardTransactionRepository;
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
        .approvedAmountCents(0L)
        .initialAmountCents(0L)
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
public Mono<Void> sendRewardBatch(String merchantId, String batchId) {
  return rewardBatchRepository.findById(batchId)
      .switchIfEmpty(Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
          ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND)))
      .flatMap(batch -> {
        if (!merchantId.equals(batch.getMerchantId())) {
          log.warn("[SEND_REWARD_BATCHES] Merchant id mismatch !");
          return Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
              ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND));
        }
        if (batch.getStatus() != RewardBatchStatus.CREATED) {
          return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
              ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST));
        }
        YearMonth batchMonth = YearMonth.parse(batch.getMonth());
        if (!YearMonth.now().isAfter(batchMonth)) {
          log.warn("[SEND_REWARD_BATCHES] Batch month too early to be sent !");
          return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
              ExceptionConstants.ExceptionCode.REWARD_BATCH_MONTH_TOO_EARLY));
        }
        batch.setStatus(RewardBatchStatus.SENT);
        batch.setUpdateDate(LocalDateTime.now());
        return rewardBatchRepository.save(batch)
            .then(rewardTransactionRepository.rewardTransactionsByBatchId(batchId))
            .then();
      });
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




    @Override
    public Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId)      {
    Mono<RewardBatch> monoRewardBatch = rewardBatchRepository.findRewardBatchById(rewardBatchId);
    return monoRewardBatch.filter(rewardBatch -> !rewardBatch.getStatus().equals(RewardBatchStatus.APPROVED))
            .map(rewardBatch -> {
              rewardBatch.setStatus(RewardBatchStatus.APPROVED);
              rewardBatch.setUpdateDate(LocalDateTime.now());
              return rewardBatch;
            })
            .flatMap(rewardBatchRepository::save)
            .flatMap(savedBatch -> {
                Mono<Void> transactionsUpdate = updateAndSaveRewardTransactionsToApprove(rewardBatchId, initiativeId);
                return transactionsUpdate.thenReturn(savedBatch);

            })
            .flatMap(savedBatch -> {
                if (savedBatch.getNumberOfTransactionsSuspended() != null && savedBatch.getNumberOfTransactionsSuspended() > 0) {
                    return createRewardBatchAndSave(savedBatch)
                            .flatMap(newBatch -> updateAndSaveRewardTransactionsSuspended(rewardBatchId, initiativeId, newBatch.getId()).thenReturn(newBatch));
                } else {
                    return Mono.just(savedBatch);
                }
            }).switchIfEmpty(Mono.error(new NoSuchElementException(
                    "RewardBatch non trovato o già approvato: " + rewardBatchId
            )));
  }
  Mono<RewardBatch> createRewardBatchAndSave(RewardBatch savedBatch) {

      Mono<RewardBatch> existingBatchMono = rewardBatchRepository.findRewardBatchByFilter(
              null,
              savedBatch.getMerchantId(),
              savedBatch.getPosType() != null ? savedBatch.getPosType().toString() : null,
              addOneMonth(savedBatch.getMonth()));

      Mono<RewardBatch> newBatchCreationMono = Mono.just(savedBatch)
              .map(batch -> RewardBatch.builder()
                .id(null)
                .merchantId(savedBatch.getMerchantId())
                .businessName(savedBatch.getBusinessName())
                .month(addOneMonth(savedBatch.getMonth()))
                .posType(savedBatch.getPosType())
                .status(RewardBatchStatus.CREATED)
                .partial(savedBatch.getPartial())
                .name(addOneMonthToItalian(savedBatch.getName()))
                .startDate(savedBatch.getStartDate())
                .endDate(savedBatch.getEndDate())
                .approvedAmountCents(0L)
                .initialAmountCents(0L)
                .numberOfTransactions(savedBatch.getNumberOfTransactionsSuspended())
                .numberOfTransactionsElaborated(0L)
                .numberOfTransactionsSuspended(savedBatch.getNumberOfTransactionsSuspended())
                .numberOfTransactionsRejected(0L)
                .reportPath(savedBatch.getReportPath())
                .assigneeLevel(RewardBatchAssignee.L1)
                .creationDate(LocalDateTime.now())
                .updateDate(LocalDateTime.now())
                .build()
              )
              .flatMap(rewardBatchRepository::save);

      return existingBatchMono
              .switchIfEmpty(newBatchCreationMono)
              .map(foundOrNewBatch -> foundOrNewBatch);

  }

    public String addOneMonth(String yearMonthString) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
        YearMonth yearMonth = YearMonth.parse(yearMonthString, inputFormatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(inputFormatter);
    }


    public String addOneMonthToItalian(String italianMonthString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM yyyy", Locale.ITALIAN);
        YearMonth yearMonth = YearMonth.parse(italianMonthString, formatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(formatter);
    }

    public  Mono<Void> updateAndSaveRewardTransactionsToApprove(String oldBatchId, String initiativeId) {
      List<RewardBatchTrxStatus>  statusList = new ArrayList<>();
      statusList.add(RewardBatchTrxStatus.TO_CHECK);
      statusList.add(RewardBatchTrxStatus.CONSULTABLE);

     return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
             .flatMap(rewardTransaction -> {
               rewardTransaction.setStatus(RewardBatchTrxStatus.APPROVED.toString());
               return rewardTransactionRepository.save(rewardTransaction);
             })
             .then(); // Converte Flux<RewardTransaction> in Mono<Void>

    }

    public  Mono<Void> updateAndSaveRewardTransactionsSuspended(String oldBatchId, String initiativeId, String newBatchId) {
        List<RewardBatchTrxStatus> statusList = new ArrayList<>();
        statusList.add(RewardBatchTrxStatus.SUSPENDED);

        return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchId(newBatchId);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then(); // Converte Flux<RewardTransaction> in Mono<Void>

    }


    @Data
    static class TotalAmount {
        private long total;
    }
}
