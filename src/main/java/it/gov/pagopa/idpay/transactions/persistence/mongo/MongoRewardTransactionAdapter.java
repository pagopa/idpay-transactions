package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoicedTransactionAssignmentPort;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoiceTransactionLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSynchronizationPort;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardTransactionAdapter implements
        RewardTransactionSynchronizationPort,
        InvoicedTransactionAssignmentPort,
        RewardBatchTransactionReadPort,
        InvoiceTransactionLookupPort {

    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Mono<RewardTransaction> upsert(RewardTransaction transaction) {
        return rewardTransactionRepository.save(transaction);
    }

    @Override
    public Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(int batchSize) {
        return rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(batchSize);
    }

    @Override
    public Mono<RewardTransaction> findInvoicedTransactionWithoutBatch(String transactionId) {
        return rewardTransactionRepository.findById(transactionId)
                .flatMap(transaction -> rewardTransactionRepository.findInvoicedTrxByIdWithoutBatch(
                        transaction.getInitiatives().getFirst(),
                        transaction.getMerchantId(),
                        transactionId
                ));
    }

    @Override
    public Flux<RewardTransaction> findBatchTransactions(
            String rewardBatchId,
            String initiativeId,
            List<RewardBatchTrxStatus> statuses
    ) {
        return rewardTransactionRepository.findByFilter(rewardBatchId, initiativeId, statuses);
    }

    @Override
    public Mono<RewardTransaction> findTransactionInBatch(
            String initiativeId,
            String merchantId,
            String rewardBatchId,
            String transactionId
    ) {
        return rewardTransactionRepository.findTransactionInBatch(
                initiativeId,
                merchantId,
                rewardBatchId,
                transactionId
        );
    }

    @Override
    public Mono<RewardTransaction> findInvoiceTransaction(String merchantId, String transactionId) {
        return rewardTransactionRepository.findTransaction(merchantId, transactionId);
    }
}
