package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionAtomicMutationPort;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardTransactionAtomicMutationAdapter
        implements RewardTransactionAtomicMutationPort {

    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String batchId,
            String transactionId,
            RewardBatchTrxStatus status,
            ReasonDTO reasons,
            String batchMonth,
            ChecksError checksError
    ) {
        return rewardTransactionRepository.updateStatusAndReturnOld(
                initiativeId,
                batchId,
                transactionId,
                status,
                reasons,
                batchMonth,
                checksError
        );
    }
}
