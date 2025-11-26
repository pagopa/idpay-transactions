package it.gov.pagopa.idpay.transactions.repository;

import com.nimbusds.oauth2.sdk.util.StringUtils;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatch.Fields;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.time.LocalDateTime;
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
  public Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, String initiativeId, String status, String assignedOperator, Pageable pageable) {
    Criteria criteria = getCriteria(merchantId, initiativeId, status, assignedOperator);

    Query query = Query.query(criteria).with(getPageableRewardBatch(pageable));
    return mongoTemplate.find(query, RewardBatch.class);
  }

  @Override
  public Flux<RewardBatch> findRewardBatch(String status, String assignedOperator, Pageable pageable) {
    Criteria criteria = new Criteria();

    Query query = Query.query(criteria).with(getPageableRewardBatch(pageable));
    return mongoTemplate.find(query, RewardBatch.class);
  }

  private static Criteria getCriteria(String merchantId, String initiativeId, String status, String assignedOperator) {
    Criteria criteria = Criteria.where(RewardTransaction.Fields.merchantId).is(merchantId)
        .and(RewardTransaction.Fields.initiatives).is(initiativeId);

    if (StringUtils.isNotBlank(assignedOperator)) {
      criteria.and(RewardBatch.Fields.assigneeLevel).is(assignedOperator);
    } else {
      criteria.and(Fields.assigneeLevel).in("L1", "L2", "L3");
    }

    if (StringUtils.isNotBlank(status)) {
      criteria.and(RewardBatch.Fields.status).is(status);
    } else {
      criteria.and(Fields.status).in("CREATED", "SENT", "EVALUATING", "APPROVED");
    }

    return criteria;
  }

  @Override
  public Mono<Long> getCount(String merchantId, String initiativeId, String status, String assignedOperator) {
    Criteria criteria = getCriteria(merchantId, initiativeId, status, assignedOperator);

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
  public Mono<Long> getCount(String status, String assignedOperator) {
    Criteria criteria = new Criteria();

    return mongoTemplate.count(Query.query(criteria), RewardBatch.class);
  }

  private Pageable getPageableRewardBatch(Pageable pageable) {
    if (pageable == null || pageable.getSort().isUnsorted()) {
      return PageRequest.of(0, 10, Sort.by("month").ascending());
    }
    return PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
  }
}
