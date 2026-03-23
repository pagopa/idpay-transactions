package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.mongodb.client.result.DeleteResult;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;

@Service
@Slf4j
@AllArgsConstructor
public class DeleteEmptyRewardBatchesUseCase {

    private final ReactiveMongoTemplate reactiveMongoTemplate;

    public Mono<Void> execute() {

        String currentMonth = LocalDate.now()
                .withDayOfMonth(1)
                .toString()
                .substring(0, 7);

        Query toDeleteQuery = Query.query(new Criteria().andOperator(
                Criteria.where(RewardBatch.Fields.numberOfTransactions).in(0L, 0),
                Criteria.where(RewardBatch.Fields.month).lt(currentMonth)
        ));

        return reactiveMongoTemplate.getMongoDatabase()
                .doOnNext(db -> log.info("[CANCEL_EMPTY_BATCHES] DB={}", db.getName()))
                .then(Mono.fromCallable(() -> reactiveMongoTemplate.getCollectionName(RewardBatch.class))
                        .doOnNext(c -> log.info("[CANCEL_EMPTY_BATCHES] Collection={}", c))
                )
                .then(reactiveMongoTemplate.count(new Query(), RewardBatch.class)
                        .doOnNext(total -> log.info("[CANCEL_EMPTY_BATCHES] Total docs={}", total))
                )
                .then(reactiveMongoTemplate.count(toDeleteQuery, RewardBatch.class)
                        .doOnNext(match -> log.info("[CANCEL_EMPTY_BATCHES] Matching docs={}", match))
                )
                .thenMany(reactiveMongoTemplate.find(toDeleteQuery, RewardBatch.class)
                        .doOnNext(b -> log.info("[CANCEL_EMPTY_BATCHES] WILL DELETE id={} month={} nTrx={}",
                                b.getId(), b.getMonth(), b.getNumberOfTransactions()))
                )
                .concatMap(b ->
                        reactiveMongoTemplate.remove(
                                        Query.query(Criteria.where("_id").is(b.getId())),
                                        RewardBatch.class
                                )
                                .map(DeleteResult::getDeletedCount)
                )
                .reduce(0L, Long::sum)
                .doOnNext(count -> log.info("[CANCEL_EMPTY_BATCHES] Deleted {} empty batches", count))
                .then();
    }
}

