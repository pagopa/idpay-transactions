package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.exception.NotEnoughFiltersException;
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
    public Flux<RewardTransaction> findByFilters(String idTrxIssuer, String userId, LocalDateTime trxDate, BigDecimal amount) {
        return mongoTemplate.find(
                Query.query(getCriteriaFilter(idTrxIssuer, userId, trxDate, amount)),
                RewardTransaction.class);
    }

    private Criteria getCriteriaFilter(String idTrxIssuer, String userId, LocalDateTime trxDate, BigDecimal amount) {
        if(idTrxIssuer !=null) {
            Criteria criteria = Criteria.where(
                    RewardTransaction.Fields.idTrxIssuer).is(idTrxIssuer);
            if (userId != null) {criteria.and(RewardTransaction.Fields.userId).is(userId);}
            if (trxDate != null) {criteria.and(RewardTransaction.Fields.trxDate).is(trxDate);}
            if (amount != null) {criteria.and(RewardTransaction.Fields.amount).is(amount);}
            return criteria;
        }else if(userId != null && trxDate!=null && amount!= null){
            return Criteria.where(
                   RewardTransaction.Fields.userId).is(userId)
                    .and(RewardTransaction.Fields.trxDate).is(trxDate)
                    .and(RewardTransaction.Fields.amount).is(amount);
        }else {
            throw new NotEnoughFiltersException("The minimum set of filter are: 1) idTrxIssuer  2) The set: userId, trxDate and amount");
        }
    }
}
