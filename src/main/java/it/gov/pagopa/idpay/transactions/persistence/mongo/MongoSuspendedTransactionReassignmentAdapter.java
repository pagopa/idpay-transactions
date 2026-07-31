package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionMutationPort;
import it.gov.pagopa.idpay.transactions.persistence.port.SuspendedTransactionReassignmentPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoSuspendedTransactionReassignmentAdapter implements SuspendedTransactionReassignmentPort {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardBatchTransactionMutationPort rewardBatchTransactionMutationPort;

    @Override
    public Mono<Void> reassignSuspendedTransactions(String sourceBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(sourceBatchId, initiativeId)
                .filter(MongoSuspendedTransactionReassignmentAdapter::hasSuspendedTransactions)
                .flatMap(source -> findOrCreateTargetBatch(source)
                        .flatMap(target -> rewardBatchTransactionMutationPort.reassignSuspendedTransactions(
                                source,
                                target,
                                initiativeId,
                                source.getMonth()
                        )))
                .then();
    }

    private Mono<RewardBatch> findOrCreateTargetBatch(RewardBatch source) {
        RewardBatch target = RewardBatchFactory.create(
                source.getInitiativeId(),
                source.getMerchantId(),
                source.getPosType(),
                targetMonth(source.getMonth()),
                source.getBusinessName()
        );
        return rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                        target.getInitiativeId(),
                        target.getMerchantId(),
                        target.getPosType(),
                        target.getMonth()
                )
                .switchIfEmpty(rewardBatchRepository.save(target)
                        .onErrorResume(DuplicateKeyException.class, ignored ->
                                rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                                        target.getInitiativeId(),
                                        target.getMerchantId(),
                                        target.getPosType(),
                                        target.getMonth()
                                )));
    }

    private static boolean hasSuspendedTransactions(RewardBatch batch) {
        return batch.getNumberOfTransactionsSuspended() != null
                && batch.getNumberOfTransactionsSuspended() > 0;
    }

    private static String targetMonth(String sourceMonth) {
        YearMonth source = YearMonth.parse(sourceMonth);
        YearMonth current = YearMonth.now(ZONEID);
        return source.isAfter(current) ? source.toString() : current.toString();
    }
}
