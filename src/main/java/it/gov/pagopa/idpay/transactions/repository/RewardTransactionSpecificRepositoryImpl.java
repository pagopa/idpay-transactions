package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
public class RewardTransactionSpecificRepositoryImpl implements RewardTransactionSpecificRepository{
    private final ReactiveMongoTemplate mongoTemplate;

    public RewardTransactionSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount, Pageable pageable) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.idTrxIssuer).is(idTrxIssuer);
        if (userId != null) {criteria.and(RewardTransaction.Fields.userId).is(userId);}
        if (amount != null) {criteria.and(RewardTransaction.Fields.amount).is(amount);}
        if(trxDateStart != null && trxDateEnd != null){
            criteria.and(RewardTransaction.Fields.trxDate)
                .gte(trxDateStart)
                .lte(trxDateEnd);
        }else if(trxDateStart != null){
            criteria.and(RewardTransaction.Fields.trxDate)
                    .gte(trxDateStart);
        }else if(trxDateEnd != null){
            criteria.and(RewardTransaction.Fields.trxDate)
                    .lte(trxDateEnd);
        }

        return mongoTemplate.find(
                Query.query(criteria)
                        .with(getPageable(pageable)),
                RewardTransaction.class);
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount, Pageable pageable) {
        Criteria criteria = Criteria
                .where(RewardTransaction.Fields.userId).is(userId)
                .and(RewardTransaction.Fields.trxDate)
                .gte(trxDateStart)
                .lte(trxDateEnd);
        if(amount != null){criteria.and(RewardTransaction.Fields.amount).is(amount);}

        return mongoTemplate.find(
                Query.query(criteria)
                        .with(getPageable(pageable)),
                RewardTransaction.class);
    }

    private Pageable getPageable(Pageable pageable){
        if (pageable == null) {
            pageable = Pageable.unpaged();
        }
        return pageable;
    }

    private Criteria getCriteria(String merchantId, String initiativeId, String userId, String status) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.merchantId).is(merchantId)
                .and(RewardTransaction.Fields.initiatives).is(initiativeId);
        if (userId != null) {
            criteria.and(RewardTransaction.Fields.userId).is(userId);
        }
        if (status != null) {
            criteria.and(RewardTransaction.Fields.status).is(status);
        } else {
            criteria.and(RewardTransaction.Fields.status).in("CANCELLED", "REWARDED");
        }
        return criteria;
    }

    @Override
    public Flux<RewardTransaction> findByFilter(String merchantId, String initiativeId, String userId, String status, Pageable pageable){
        Criteria criteria = getCriteria(merchantId, initiativeId, userId, status);
        return mongoTemplate.find(Query.query(criteria).with(getPageable(pageable)), RewardTransaction.class);
    }

    @Override
    public Mono<Long> getCount(String merchantId, String initiativeId, String userId, String status) {
        Criteria criteria = getCriteria(merchantId, initiativeId, userId, status);
        return mongoTemplate.count(Query.query(criteria), RewardTransaction.class);
    }
}
