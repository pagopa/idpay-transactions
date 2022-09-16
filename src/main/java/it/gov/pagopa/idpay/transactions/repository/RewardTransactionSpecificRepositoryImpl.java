package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;

public class RewardTransactionSpecificRepositoryImpl implements RewardTransactionSpecificRepository{
    private final ReactiveMongoTemplate mongoTemplate;

    public RewardTransactionSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Flux<RewardTransaction> findAllInRange(LocalDateTime start, LocalDateTime end) {
        return mongoTemplate.find(
                Query.query(
                        Criteria.where(RewardTransaction.Fields.trxDate)
                                .gte(start)
                                .lte(end)
                ),
                RewardTransaction.class);
    }

}
