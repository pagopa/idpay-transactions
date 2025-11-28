package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
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
  public Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, Pageable pageable){

    Criteria criteria = getCriteria(merchantId);

    return mongoTemplate.find(
        Query.query(criteria).with(getPageableRewardBatch(pageable)),
        RewardBatch.class);

  }

  @Override
  public Flux<RewardBatch> findRewardBatch(Pageable pageable){

    Query query = new Query().with(getPageableRewardBatch(pageable));

    return mongoTemplate.find(query, RewardBatch.class);

  }

  private static Criteria getCriteria(String merchantId) {
    return Criteria.where(RewardBatch.Fields.merchantId).is(merchantId);
  }

  @Override
  public Mono<Long> getCount(String merchantId) {
    Criteria criteria = getCriteria(merchantId);

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
  public Mono<Long> getCount() {

    return mongoTemplate.count(new Query(), RewardBatch.class);
  }

  private Pageable getPageableRewardBatch(Pageable pageable) {
    if (pageable == null || pageable.getSort().isUnsorted()) {
      return PageRequest.of(0, 10, Sort.by("month").ascending());
    }
    return PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
  }
}
