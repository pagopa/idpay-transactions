package it.gov.pagopa.idpay.transactions.repository;

import com.nimbusds.oauth2.sdk.util.StringUtils;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatch.Fields;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
  public Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, String status, String assigneeLevel, Pageable pageable) {
    Criteria criteria = getCriteria(merchantId, status, assigneeLevel);

    Query query = Query.query(criteria).with(getPageableRewardBatch(pageable));
    return mongoTemplate.find(query, RewardBatch.class);
  }

  @Override
  public Flux<RewardBatch> findRewardBatch(String status, String assigneeLevel, Pageable pageable) {
    Criteria criteria = buildCriteria(status, assigneeLevel);
    Query query = Query.query(criteria).with(getPageableRewardBatch(pageable));
    return mongoTemplate.find(query, RewardBatch.class);
  }

  private static Criteria getCriteria(String merchantId, String status, String assigneeLevel) {
    Criteria criteria = Criteria.where(RewardBatch.Fields.merchantId).is(merchantId);

    if (StringUtils.isNotBlank(assigneeLevel)) {
      criteria.and(RewardBatch.Fields.assigneeLevel).is(assigneeLevel);
    } else {
      criteria.and(Fields.assigneeLevel).in(
          RewardBatchAssignee.L1,
          RewardBatchAssignee.L2,
          RewardBatchAssignee.L3
      );
    }

    if (StringUtils.isNotBlank(status)) {
      criteria.and(RewardBatch.Fields.status).is(status);
    } else {
      criteria.and(Fields.status).in(
          RewardBatchStatus.CREATED,
          RewardBatchStatus.SENT,
          RewardBatchStatus.EVALUATING,
          RewardBatchStatus.APPROVED
      );
    }

    return criteria;
  }

  private Criteria buildCriteria(String status, String assigneeLevel) {

    List<Criteria> subCriteria = new ArrayList<>();

    if (StringUtils.isNotBlank(assigneeLevel)) {
      subCriteria.add(Criteria.where(RewardBatch.Fields.assigneeLevel).is(RewardBatchAssignee.valueOf(assigneeLevel))
      );
    } else {
      subCriteria.add(
          Criteria.where(RewardBatch.Fields.assigneeLevel)
              .in(RewardBatchAssignee.L1, RewardBatchAssignee.L2, RewardBatchAssignee.L3)
      );
    }

    if (StringUtils.isNotBlank(status)) {
      subCriteria.add(
          Criteria.where(RewardBatch.Fields.status)
              .is(RewardBatchStatus.valueOf(status))
      );
    } else {
      subCriteria.add(
          Criteria.where(RewardBatch.Fields.status)
              .in(
                  RewardBatchStatus.CREATED,
                  RewardBatchStatus.SENT,
                  RewardBatchStatus.EVALUATING,
                  RewardBatchStatus.APPROVED
              )
      );
    }

    return new Criteria().andOperator(subCriteria.toArray(new Criteria[0]));
  }


  @Override
  public Mono<Long> getCount(String merchantId, String status, String assigneeLevel) {
    Criteria criteria = getCriteria(merchantId, status, assigneeLevel);

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
  public Mono<Long> getCount(String status, String assigneeLevel) {
    Criteria criteria = buildCriteria(status, assigneeLevel);
    return mongoTemplate.count(Query.query(criteria), RewardBatch.class);
  }

  private Pageable getPageableRewardBatch(Pageable pageable) {
    if (pageable == null || pageable.getSort().isUnsorted()) {
      return PageRequest.of(0, 10, Sort.by("month").ascending());
    }
    return PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
  }
}
