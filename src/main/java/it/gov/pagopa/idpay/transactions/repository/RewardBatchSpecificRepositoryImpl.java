package it.gov.pagopa.idpay.transactions.repository;

import com.nimbusds.oauth2.sdk.util.StringUtils;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
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

public class RewardBatchSpecificRepositoryImpl implements RewardBatchSpecificRepository {

  private final ReactiveMongoTemplate mongoTemplate;

  public RewardBatchSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate){
    this.mongoTemplate = mongoTemplate;
  }

  @Override
  public Flux<RewardBatch> findRewardBatchesCombined(String merchantId, String status, String assigneeLevel, boolean isOperator, Pageable pageable) {
    Criteria criteria = buildCombinedCriteria(merchantId, status, assigneeLevel, isOperator);
    Query query = Query.query(criteria).with(getPageableRewardBatch(pageable));
    return mongoTemplate.find(query, RewardBatch.class);
  }

  @Override
  public Mono<Long> getCountCombined(String merchantId, String status, String assigneeLevel, boolean isOperator) {
    Criteria criteria = buildCombinedCriteria(merchantId, status, assigneeLevel, isOperator);
    return mongoTemplate.count(Query.query(criteria), RewardBatch.class);
  }

  @Override
  public Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents) {
    return mongoTemplate.findAndModify(
        Query.query(Criteria.where("_id").is(batchId)),
        new Update()
            .inc("initialAmountCents", accruedAmountCents)
            .inc("numberOfTransactions", 1)
            .set("updateDate", LocalDateTime.now()),
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
            .set("updateDate", LocalDateTime.now()),
        FindAndModifyOptions.options().returnNew(true),
        RewardBatch.class
    );
  }

  private Criteria buildCombinedCriteria(String merchantId, String status, String assigneeLevel, boolean isOperator) {
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
        RewardBatchStatus.APPROVED
    )
        : EnumSet.of(
            RewardBatchStatus.CREATED,
            RewardBatchStatus.SENT,
            RewardBatchStatus.EVALUATING,
            RewardBatchStatus.APPROVED
        );

    if (StringUtils.isBlank(status)) {
      subCriteria.add(
          Criteria.where(RewardBatch.Fields.status)
              .in(allowedStatuses)
      );
    } else {
      RewardBatchStatus requestedStatus = RewardBatchStatus.valueOf(status);

      if (!allowedStatuses.contains(requestedStatus)) {
        subCriteria.add(
            Criteria.where(RewardBatch.Fields.id).in(Collections.emptyList())
        );
      } else {
        subCriteria.add(
            Criteria.where(RewardBatch.Fields.status)
                .is(requestedStatus)
        );
      }
    }

    return new Criteria().andOperator(subCriteria.toArray(new Criteria[0]));
}

  private Pageable getPageableRewardBatch(Pageable pageable) {
    if (pageable == null || pageable.getSort().isUnsorted()) {
      return PageRequest.of(0, 10, Sort.by("month").ascending());
    }
    return PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
  }
}
