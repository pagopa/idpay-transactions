package it.gov.pagopa.idpay.transactions.persistence.mongo;

import com.mongodb.client.result.DeleteResult;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchLifecyclePort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;

@Component
@RequiredArgsConstructor
@Slf4j
public class MongoRewardBatchLifecycleAdapter implements RewardBatchLifecyclePort {

    private final RewardBatchRepository rewardBatchRepository;
    private final ReactiveMongoTemplate reactiveMongoTemplate;

    @Override
    public Mono<RewardBatch> findBatch(String rewardBatchId) {
        return rewardBatchRepository.findById(rewardBatchId);
    }

    @Override
    public Mono<RewardBatch> findBatch(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId);
    }

    @Override
    public Mono<RewardBatch> findBatchWithStatus(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status
    ) {
        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                rewardBatchId,
                initiativeId,
                status
        );
    }

    @Override
    public Flux<RewardBatch> findBatchesWithStatus(RewardBatchStatus status, String initiativeId) {
        return rewardBatchRepository.findByStatusAndInitiativeId(status, initiativeId);
    }

    @Override
    public Flux<RewardBatch> findBatchesWithStatus(
            RewardBatchStatus status,
            String initiativeId,
            Pageable pageable
    ) {
        return rewardBatchRepository.findByStatusAndInitiativeId(status, initiativeId, pageable);
    }

    @Override
    public Flux<RewardBatch> findDeliverableBatches(String initiativeId, Pageable pageable) {
        return rewardBatchRepository.findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                RewardBatchStatus.APPROVED,
                initiativeId,
                0L,
                pageable
        );
    }

    @Override
    public Flux<RewardBatch> findMerchantBatches(
            String merchantId,
            String initiativeId,
            PosType posType
    ) {
        return rewardBatchRepository.findByMerchantIdAndInitiativeIdAndPosType(
                merchantId,
                initiativeId,
                posType
        );
    }

    @Override
    public Mono<RewardBatch> saveBatch(RewardBatch rewardBatch) {
        return rewardBatchRepository.save(rewardBatch);
    }

    @Override
    public Mono<RewardBatch> updateEvaluationStatus(
            String rewardBatchId,
            String initiativeId,
            long approvedAmountCents
    ) {
        return rewardBatchRepository.updateStatusAndApprovedAmountCents(
                rewardBatchId,
                RewardBatchStatus.EVALUATING,
                approvedAmountCents,
                initiativeId
        );
    }

    @Override
    public Mono<Void> deleteEmptyBatches() {
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
                        .doOnNext(collection -> log.info("[CANCEL_EMPTY_BATCHES] Collection={}", collection))
                )
                .then(reactiveMongoTemplate.count(new Query(), RewardBatch.class)
                        .doOnNext(total -> log.info("[CANCEL_EMPTY_BATCHES] Total docs={}", total))
                )
                .then(reactiveMongoTemplate.count(toDeleteQuery, RewardBatch.class)
                        .doOnNext(matches -> log.info("[CANCEL_EMPTY_BATCHES] Matching docs={}", matches))
                )
                .thenMany(reactiveMongoTemplate.find(toDeleteQuery, RewardBatch.class)
                        .doOnNext(rewardBatch ->
                                log.info("[CANCEL_EMPTY_BATCHES] WILL DELETE rewardBatch={}", rewardBatch)
                        )
                )
                .concatMap(rewardBatch ->
                        reactiveMongoTemplate.remove(
                                        Query.query(Criteria.where("_id").is(rewardBatch.getId())),
                                        RewardBatch.class
                                )
                                .map(DeleteResult::getDeletedCount)
                )
                .reduce(0L, Long::sum)
                .doOnNext(count -> log.info("[CANCEL_EMPTY_BATCHES] Deleted {} empty batches", count))
                .then();
    }
}
