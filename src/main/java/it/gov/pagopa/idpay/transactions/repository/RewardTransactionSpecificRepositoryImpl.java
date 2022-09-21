package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
public class RewardTransactionSpecificRepositoryImpl implements RewardTransactionSpecificRepository{
    private final ReactiveMongoTemplate mongoTemplate;

    public RewardTransactionSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuerAndOtherFilters(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.idTrxIssuer).is(idTrxIssuer);
        if(trxDateStart != null){criteria.and(RewardTransaction.Fields.trxDate).gte(trxDateStart);}
        if(trxDateEnd != null){criteria.and(RewardTransaction.Fields.trxDate).lte(trxDateEnd);}
        if (userId != null) {criteria.and(RewardTransaction.Fields.userId).is(userId);}
        if (amount != null) {criteria.and(RewardTransaction.Fields.amount).is(amount);}

        return mongoTemplate.find(
                Query.query(criteria),
                RewardTransaction.class);
    }

    @Override
    public Flux<RewardTransaction> findByUserIdAndRangeDateAndAmount(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount) {
        Criteria criteria = Criteria
                .where(RewardTransaction.Fields.userId).is(userId)
                .and(RewardTransaction.Fields.trxDate)
                .gte(trxDateStart)
                .lte(trxDateEnd);
        if(amount != null){criteria.and(RewardTransaction.Fields.amount).is(amount);}

        return mongoTemplate.find(
                Query.query(criteria),
                RewardTransaction.class);
    }
}
