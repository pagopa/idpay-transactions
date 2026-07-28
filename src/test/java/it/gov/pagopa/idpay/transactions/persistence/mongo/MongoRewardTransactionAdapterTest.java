package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
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

    @Mock
    private RewardBatchRepository rewardBatchRepository;

    @Test
    void upsertDelegatesKafkaTransactionPersistence() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction transaction = transaction();
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.upsert(transaction))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardTransactionRepository).save(transaction);
    }

    @Test
    void assignmentLookupsPreserveUnassignedInvoicedQueries() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
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
    void assignmentDoesNotSaveWhenTheBatchCounterUpdateDoesNotMatch() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction transaction = transaction();
        transaction.setPointOfSaleType(PosType.PHYSICAL);
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .posType(PosType.PHYSICAL)
                .month("2026-07")
                .status(RewardBatchStatus.CREATED)
                .build();

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07"
        )).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any()))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch, 42))
                .verifyComplete();

        assertNull(transaction.getRewardBatchId());
        verify(rewardTransactionRepository, never()).save(transaction);
    }

    @Test
    void assignmentPersistsMembershipAndDerivedMongoCounters() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction transaction = transaction();
        transaction.setPointOfSaleType(PosType.PHYSICAL);
        transaction.setRewards(Map.of(
                INITIATIVE_ID,
                Reward.builder().accruedRewardCents(250L).build()
        ));
        RewardBatch batch = createdBatch();

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07"
        )).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.updateTotals(
                eq(INITIATIVE_ID),
                eq(BATCH_ID),
                argThat(counters -> counters.getInitialAmountCents().equals(250L)
                        && counters.getNumberOfTransactions().equals(1L))
        )).thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch, 42))
                .assertNext(assigned -> {
                    assertEquals(BATCH_ID, assigned.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, assigned.getRewardBatchTrxStatus());
                    assertEquals(42, assigned.getSamplingKey());
                    assertNotNull(assigned.getRewardBatchInclusionDate());
                })
                .verifyComplete();

        verify(rewardTransactionRepository).save(transaction);
    }

    @Test
    void assignmentCreatesAMissingBatchBeforePersistingTheTransaction() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction transaction = transaction();
        transaction.setPointOfSaleType(PosType.PHYSICAL);
        RewardBatch batch = createdBatch();

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07"
        )).thenReturn(Mono.empty());
        when(rewardBatchRepository.save(batch)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any()))
                .thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch, 42))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardBatchRepository).save(batch);
    }

    @Test
    void assignmentReadsTheBatchAfterADuplicateKeyRace() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction transaction = transaction();
        transaction.setPointOfSaleType(PosType.PHYSICAL);
        RewardBatch batch = createdBatch();

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07"
        )).thenReturn(Mono.empty(), Mono.just(batch));
        when(rewardBatchRepository.save(batch)).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any()))
                .thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch, 42))
                .expectNext(transaction)
                .verifyComplete();

        verify(rewardBatchRepository).save(batch);
    }

    @Test
    void assignmentRejectsABatchThatIsNoLongerCreated() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction transaction = transaction();
        transaction.setPointOfSaleType(PosType.PHYSICAL);
        RewardBatch batch = createdBatch();
        batch.setStatus(RewardBatchStatus.SENT);

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07"
        )).thenReturn(Mono.just(batch));

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch, 42))
                .expectErrorMatches(error -> error instanceof ClientExceptionNoBody exception
                        && exception.getHttpStatus().is4xxClientError())
                .verify();

        verify(rewardBatchRepository, never()).updateTotals(any(), any(), any());
        verify(rewardTransactionRepository, never()).save(transaction);
    }

    @Test
    void assignmentRejectsABatchFromAnotherInitiative() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardBatch batch = RewardBatch.builder()
                .initiativeId("other-initiative")
                .merchantId(MERCHANT_ID)
                .posType(PosType.PHYSICAL)
                .month("2026-07")
                .status(RewardBatchStatus.CREATED)
                .build();

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction(), batch, 42))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("same initiative"))
                .verify();

        verify(rewardBatchRepository, never()).findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                any(),
                any(),
                any(),
                any()
        );
    }

    @Test
    void assignmentRejectsTransactionsWithoutExactlyOneNonBlankInitiative() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
        RewardTransaction withoutInitiative = transaction();
        withoutInitiative.setInitiatives(null);
        RewardTransaction withMultipleInitiatives = transaction();
        withMultipleInitiatives.setInitiatives(List.of(INITIATIVE_ID, "initiative-2"));
        RewardTransaction withBlankInitiative = transaction();
        withBlankInitiative.setInitiatives(List.of(" "));

        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withoutInitiative, createdBatch(), 42)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withMultipleInitiatives, createdBatch(), 42)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withBlankInitiative, createdBatch(), 42)
        );
    }

    @Test
    void batchTransactionReadsDelegateStatusesAndBatchIdentity() {
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
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
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
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
        MongoRewardTransactionAdapter adapter = new MongoRewardTransactionAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
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

    private RewardBatch createdBatch() {
        return RewardBatch.builder()
                .id(BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .posType(PosType.PHYSICAL)
                .month("2026-07")
                .status(RewardBatchStatus.CREATED)
                .build();
    }
}
