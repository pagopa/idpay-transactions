package it.gov.pagopa.idpay.transactions.repository;

import com.nimbusds.oauth2.sdk.util.StringUtils;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class RewardBatchSpecificRepositoryImpl implements RewardBatchSpecificRepository {

  private final ReactiveMongoTemplate mongoTemplate;

  public RewardBatchSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
    this.mongoTemplate = mongoTemplate;
  }

  @Override
  public Flux<RewardBatch> findRewardBatchesCombined(String merchantId, String status, String assigneeLevel, String businessName, String month, boolean isOperator, Pageable pageable) {
    Criteria criteria = buildCombinedCriteria(merchantId, status, assigneeLevel, businessName, month, isOperator);
    Query query = Query.query(criteria).with(getPageableRewardBatch(pageable));
    return mongoTemplate.find(query, RewardBatch.class);
  }

  @Override
  public Mono<Long> getCountCombined(String merchantId, String status, String assigneeLevel, String businessName, String month, boolean isOperator) {
    Criteria criteria = buildCombinedCriteria(merchantId, status, assigneeLevel, businessName, month, isOperator);
    return mongoTemplate.count(Query.query(criteria), RewardBatch.class);
  }

  @Override
  public Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents) {
    return mongoTemplate.findAndModify(
        Query.query(Criteria.where("_id").is(batchId)),
        new Update()
            .inc("initialAmountCents", accruedAmountCents)
            .inc("numberOfTransactions", 1)
            .set(RewardBatch.Fields.updateDate, LocalDateTime.now()),
        FindAndModifyOptions.options().returnNew(true),
        RewardBatch.class
    );
  }

  @Override
  public Mono<RewardBatch> decrementTotals(String batchId, long accruedAmountCents) {
    return mongoTemplate.findAndModify(
        Query.query(Criteria.where("_id").is(batchId)),
        new Update()
            .inc("initialAmountCents", -accruedAmountCents)
            .inc("numberOfTransactions", -1)
            .set(RewardBatch.Fields.updateDate, LocalDateTime.now()),
        FindAndModifyOptions.options().returnNew(true),
        RewardBatch.class
    );
  }

  private Criteria buildCombinedCriteria(String merchantId, String status, String assigneeLevel, String businessName, String month, boolean isOperator) {
    List<Criteria> subCriteria = new ArrayList<>();

    if (StringUtils.isNotBlank(merchantId)) {
      subCriteria.add(Criteria.where(RewardBatch.Fields.merchantId).is(merchantId));
    }
    if (StringUtils.isNotBlank(assigneeLevel)) {
      subCriteria.add(
          Criteria.where(RewardBatch.Fields.assigneeLevel)
              .is(RewardBatchAssignee.valueOf(assigneeLevel))
      );
    }

    EnumSet<RewardBatchStatus> allowedStatuses = isOperator
        ? EnumSet.of(
        RewardBatchStatus.SENT,
        RewardBatchStatus.EVALUATING,
        RewardBatchStatus.APPROVING,
        RewardBatchStatus.APPROVED,
        RewardBatchStatus.TO_APPROVE,
        RewardBatchStatus.TO_WORK

    )
        : EnumSet.of(
            RewardBatchStatus.CREATED,
            RewardBatchStatus.SENT,
            RewardBatchStatus.EVALUATING,
            RewardBatchStatus.APPROVING,
            RewardBatchStatus.APPROVED,
            RewardBatchStatus.TO_APPROVE,
            RewardBatchStatus.TO_WORK
        );

    if (StringUtils.isBlank(status)) {
      subCriteria.add(
          Criteria.where(RewardBatch.Fields.status)
              .in(allowedStatuses)
      );
    } else {
      RewardBatchStatus requestedStatus = RewardBatchStatus.valueOf(status);

      if (!allowedStatuses.contains(requestedStatus)
              || StringUtils.isNotBlank(assigneeLevel)
              && (requestedStatus.equals(RewardBatchStatus.TO_APPROVE)
              && !assigneeLevel.equals(RewardBatchAssignee.L3.toString())
              || (requestedStatus.equals(RewardBatchStatus.TO_WORK)
              && assigneeLevel.equals(RewardBatchAssignee.L3.toString())))) {
        subCriteria.add(
                Criteria.where(RewardBatch.Fields.id).in(Collections.emptyList())
        );
      } else {
        if (requestedStatus.equals(RewardBatchStatus.TO_APPROVE)) {
          subCriteria.add(
                  Criteria.where(RewardBatch.Fields.status)
                          .is(RewardBatchStatus.EVALUATING)
          );
          if (StringUtils.isBlank(assigneeLevel)) {
            subCriteria.add(
                    Criteria.where(RewardBatch.Fields.assigneeLevel)
                            .is(RewardBatchAssignee.L3)
            );
          }
        } else if (requestedStatus.equals(RewardBatchStatus.TO_WORK)) {
          subCriteria.add(
                  Criteria.where(RewardBatch.Fields.status)
                          .is(RewardBatchStatus.EVALUATING)
          );
          if (StringUtils.isBlank(assigneeLevel)) {
            List<RewardBatchAssignee> allowedLevels = Arrays.asList(
                    RewardBatchAssignee.L1,
                    RewardBatchAssignee.L2
            );
            subCriteria.add(
                    Criteria.where(RewardBatch.Fields.assigneeLevel)
                            .in(allowedLevels)
            );
          }
        } else {
          subCriteria.add(
                  Criteria.where(RewardBatch.Fields.status)
                          .is(requestedStatus)
          );
        }
      }
    }
    if (StringUtils.isNotBlank(businessName)) {
      subCriteria.add(Criteria.where(RewardBatch.Fields.businessName).is(businessName));
    }
    if (StringUtils.isNotBlank(month)) {
      subCriteria.add(Criteria.where(RewardBatch.Fields.month).is(month));
    }

    return new Criteria().andOperator(subCriteria.toArray(new Criteria[0]));
}

  @Override
  public Mono<Long> updateTransactionsStatus(String rewardBatchId, List<String> transactionIds, RewardBatchTrxStatus newStatus, String reason) {

    List<String> ids = transactionIds != null ? transactionIds : List.of();

    return Flux.fromIterable(ids)
            .flatMap(id -> {
              Query q = new Query(
                      Criteria.where("rewardBatchId").is(rewardBatchId)
                              .and("id").is(id)
              );

              Update update = new Update()
                      .set("rewardBatchTrxStatus", newStatus)
                      .set("rewardBatchRejectionReason", reason);

              return mongoTemplate.findAndModify(q, update, RewardTransaction.class)
                      .map(rt -> 1L)
                      .defaultIfEmpty(0L);
            })
            .reduce(0L, Long::sum)
            .map(updatedCount -> {
              if (!ids.isEmpty() && !updatedCount.equals((long) ids.size())) {
                throw new IllegalStateException("Not all transactions were updated");
              }
              return updatedCount;
            });
  }

  @Override
  public Mono<RewardBatch> updateTotals(String rewardBatchId, long elaboratedTrxNumber, long updateAmountCents, long rejectedTrxNumber, long suspendedTrxNumber) {

    Update update = new Update();
    if (elaboratedTrxNumber != 0){
      update
              .inc("numberOfTransactionsElaborated", elaboratedTrxNumber);
    }
    if (rejectedTrxNumber != 0){
      update
       .inc("numberOfTransactionsRejected", rejectedTrxNumber);
    }
    if (suspendedTrxNumber != 0){
      update
              .inc("numberOfTransactionsSuspended", suspendedTrxNumber);
    }
    if (updateAmountCents != 0){
      update
              .inc("approvedAmountCents", updateAmountCents);
    }
          update
            .currentDate("updateDate");

    Query query = Query.query(Criteria.where("_id").is(rewardBatchId));

    return mongoTemplate.findAndModify(
            query,
            update,
            FindAndModifyOptions.options().returnNew(true),
            RewardBatch.class
    );
  }

  private Pageable getPageableRewardBatch(Pageable pageable) {
    if (pageable == null || pageable.getSort().isUnsorted()) {
      return PageRequest.of(0, 10, Sort.by("month").ascending());
    }
    return PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
  }

  @Override
  public Mono<RewardBatch> findRewardBatchById(String rewardBatchId) {
    Criteria criteria = getCriteriaFindRewardBatchById(rewardBatchId);

    return mongoTemplate.findOne(
            Query.query(criteria),
            RewardBatch.class);

  }

  @Override
  public Mono<RewardBatch> findRewardBatchByFilter(String rewardBatchId, String merchantId, PosType posType, String month) {
    Criteria criteria = getCriteriaFindRewardBatchByFilter(rewardBatchId, merchantId, posType, month);

    return mongoTemplate.findOne(
            Query.query(criteria),
            RewardBatch.class);

  }


  public Flux<RewardBatch> findRewardBatchByStatus(RewardBatchStatus rewardBatchStatus) {
    Criteria criteria = getCriteriaFindRewardBatchByStatus(rewardBatchStatus);

    return mongoTemplate.find(
            Query.query(criteria),
            RewardBatch.class);

  }

  public Flux<RewardBatch> findRewardBatchByMonthBefore(String merchantId, PosType posType, String month) {
    Criteria criteria = getCriteriaFindRewardBatchByMonthBefore(merchantId, posType, month);

    return mongoTemplate.find(
            Query.query(criteria),
            RewardBatch.class);

  }
  @Override
  public Mono<RewardBatch> updateStatusAndApprovedAmountCents(String rewardBatchId, RewardBatchStatus rewardBatchStatus, Long approvedAmountCents) {
    return mongoTemplate.findAndModify(
            Query.query(getCriteriaFindRewardBatchById(rewardBatchId)),
            new Update()
                    .set(RewardBatch.Fields.status, rewardBatchStatus)
                    .set(RewardBatch.Fields.approvedAmountCents, approvedAmountCents)
                    .set(RewardBatch.Fields.updateDate, LocalDateTime.now()),
            FindAndModifyOptions.options().returnNew(true),
            RewardBatch.class);
  }

    @Override
    public Flux<RewardBatch> findPreviousEmptyBatches() {

        String currentMonth = LocalDate.now()
                .withDayOfMonth(1)
                .toString()
                .substring(0, 7);

        Criteria criteria = new Criteria().andOperator(
                Criteria.where(RewardBatch.Fields.numberOfTransactions).in(0L, 0),
                Criteria.where(RewardBatch.Fields.month).lt(currentMonth)
        );

        Query query = Query.query(criteria)
                .with(Sort.by(Sort.Direction.ASC, RewardBatch.Fields.month));

        return mongoTemplate.find(query, RewardBatch.class);
    }




    private static Criteria getCriteriaFindRewardBatchById(String rewardBatchId) {
    return Criteria.where("_id").is(rewardBatchId.trim());
  }

  private static Criteria getCriteriaFindRewardBatchByFilter(String rewardBatchId, String merchantId, PosType posType, String month) {
    Criteria criteria;
    if(rewardBatchId != null){
      criteria = Criteria.where("_id").is(rewardBatchId.trim());
    }else{
      criteria = new Criteria();
    }
    if(merchantId != null){
      criteria.and(RewardBatch.Fields.merchantId).is(merchantId);
    }
    if(posType != null){
      criteria.and(RewardBatch.Fields.posType).is(posType);
    }
    if(month != null){
      criteria.and(RewardBatch.Fields.month).is(month);
    }
    return criteria;
  }

private static Criteria getCriteriaFindRewardBatchByStatus(RewardBatchStatus rewardBatchStatus) {
  return Criteria.where(RewardBatch.Fields.status).is(rewardBatchStatus);
}

  private static Criteria getCriteriaFindRewardBatchByMonthBefore(String merchantId, PosType posType, String month) {
    return Criteria.where(RewardBatch.Fields.merchantId).is(merchantId)
            .and(RewardBatch.Fields.posType).is(posType)
            .and(RewardBatch.Fields.month).lt(month);
  }

}
