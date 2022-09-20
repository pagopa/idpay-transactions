package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
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
    public Flux<RewardTransaction> findByFilters(String idTrxIssuer, String userId, BigDecimal amount,  LocalDateTime trxDateStart, LocalDateTime trxDateEnd) {
        return mongoTemplate.find(
                Query.query(getCriteriaFilter(idTrxIssuer, userId, amount, trxDateStart, trxDateEnd)),
                RewardTransaction.class);
    }

    private Criteria getCriteriaFilter(String idTrxIssuer, String userId, BigDecimal amount, LocalDateTime trxDateStart, LocalDateTime trxDateEnd) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.trxDate)
                .gte(trxDateStart)
                .lte(trxDateEnd);
        if(idTrxIssuer !=null) {
            criteria.and(RewardTransaction.Fields.idTrxIssuer).is(idTrxIssuer);
            if (userId != null) {criteria.and(RewardTransaction.Fields.userId).is(userId);}
            if (amount != null) {criteria.and(RewardTransaction.Fields.amount).is(amount);}
        }else if(userId != null && amount!= null){
            criteria.and(RewardTransaction.Fields.userId).is(userId)
                    .and(RewardTransaction.Fields.amount).is(amount);
        }else {
            throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,"Error", "Missing filters. Add one of the following options: 1) idTrxIssuer 2) userId and amount");
        }
        return criteria;
    }
}
