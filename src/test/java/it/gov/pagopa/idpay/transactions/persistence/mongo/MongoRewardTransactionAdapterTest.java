package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardTransactionAdapterTest {

    private static final String TRANSACTION_ID = "transaction";
    private static final String INITIATIVE_ID = "initiative";
    private static final String MERCHANT_ID = "merchant";
    private static final String BATCH_ID = "batch";

    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Test
    void upsertDelegatesKafkaTransactionPersistence() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(rewardTransactionRepository);
        RewardTransaction transaction = transaction();
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.upsert(transaction))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardTransactionRepository).save(transaction);
    }

    @Test
    void assignmentLookupsPreserveUnassignedInvoicedQueries() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(rewardTransactionRepository);
        RewardTransaction transaction = transaction();
        when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(10))
                .thenReturn(Flux.just(transaction));
        when(rewardTransactionRepository.findById(TRANSACTION_ID)).thenReturn(Mono.just(transaction));
        when(rewardTransactionRepository.findInvoicedTrxByIdWithoutBatch(
                INITIATIVE_ID, MERCHANT_ID, TRANSACTION_ID)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.findInvoicedTransactionsWithoutBatch(10))
                .expectNext(transaction)
                .verifyComplete();
        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch(TRANSACTION_ID))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardTransactionRepository).findInvoicedTransactionsWithoutBatch(10);
        verify(rewardTransactionRepository).findById(TRANSACTION_ID);
        verify(rewardTransactionRepository).findInvoicedTrxByIdWithoutBatch(
                INITIATIVE_ID, MERCHANT_ID, TRANSACTION_ID);
    }

    @Test
    void batchTransactionReadsDelegateStatusesAndBatchIdentity() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(rewardTransactionRepository);
        RewardTransaction transaction = transaction();
        List<RewardBatchTrxStatus> statuses = List.of(RewardBatchTrxStatus.CONSULTABLE);
        when(rewardTransactionRepository.findByFilter(BATCH_ID, INITIATIVE_ID, statuses))
                .thenReturn(Flux.just(transaction));
        when(rewardTransactionRepository.findTransactionInBatch(
                INITIATIVE_ID, MERCHANT_ID, BATCH_ID, TRANSACTION_ID)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.findBatchTransactions(BATCH_ID, INITIATIVE_ID, statuses))
                .expectNext(transaction)
                .verifyComplete();
        StepVerifier.create(adapter.findTransactionInBatch(
                        INITIATIVE_ID, MERCHANT_ID, BATCH_ID, TRANSACTION_ID))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardTransactionRepository).findByFilter(BATCH_ID, INITIATIVE_ID, statuses);
        verify(rewardTransactionRepository).findTransactionInBatch(
                INITIATIVE_ID, MERCHANT_ID, BATCH_ID, TRANSACTION_ID);
    }

    @Test
    void invoiceLookupDelegatesMerchantScopedTransactionQuery() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(rewardTransactionRepository);
        RewardTransaction transaction = transaction();
        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRANSACTION_ID))
                .thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.findInvoiceTransaction(MERCHANT_ID, TRANSACTION_ID))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, TRANSACTION_ID);
    }

    @Test
    void decisionMutationDelegatesAtomicStatusUpdate() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(rewardTransactionRepository);
        RewardTransaction transaction = transaction();
        ReasonDTO reason = new ReasonDTO(null, "reason");
        ChecksError checksError = new ChecksError();

        when(rewardTransactionRepository.updateStatusAndReturnOld(
                INITIATIVE_ID,
                BATCH_ID,
                TRANSACTION_ID,
                RewardBatchTrxStatus.REJECTED,
                reason,
                "2025-12",
                checksError
        )).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        TRANSACTION_ID,
                        RewardBatchTrxStatus.REJECTED,
                        reason,
                        "2025-12",
                        checksError
                ))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardTransactionRepository).updateStatusAndReturnOld(
                INITIATIVE_ID,
                BATCH_ID,
                TRANSACTION_ID,
                RewardBatchTrxStatus.REJECTED,
                reason,
                "2025-12",
                checksError
        );
    }

    private RewardTransaction transaction() {
        return RewardTransaction.builder()
                .id(TRANSACTION_ID)
                .merchantId(MERCHANT_ID)
                .initiatives(List.of(INITIATIVE_ID))
                .build();
    }
}
