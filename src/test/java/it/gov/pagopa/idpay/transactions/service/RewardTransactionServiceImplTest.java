package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleTypeEnum;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoicedTransactionAssignmentPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSearchPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSynchronizationPort;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.YearMonth;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RewardTransactionServiceImplTest {

    @Mock private RewardTransactionSearchPort searchPort;
    @Mock private RewardTransactionSynchronizationPort synchronizationPort;
    @Mock private InvoicedTransactionAssignmentPort assignmentPort;
    @Mock private MerchantRestClient merchantRestClient;

    private RewardTransactionServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new RewardTransactionServiceImpl(
                searchPort, synchronizationPort, assignmentPort, merchantRestClient, 123);
    }

    @Test
    void legacySearchMethodsDelegateToSqlSearchPort() {
        RewardTransaction transaction = RewardTransaction.builder().id("transaction").build();
        LocalDateTime from = LocalDateTime.parse("2026-01-01T00:00:00");
        LocalDateTime to = from.plusDays(1);
        when(searchPort.findByIdTrxIssuer("issuer", "user", from, to, 100L, null))
                .thenReturn(Flux.just(transaction));
        when(searchPort.findByRange("user", from, to, 100L, null))
                .thenReturn(Flux.just(transaction));
        when(searchPort.findByInitiativeIdAndUserId("initiative", "user"))
                .thenReturn(Flux.just(transaction));

        StepVerifier.create(service.findByIdTrxIssuer("issuer", "user", from, to, 100L, null))
                .expectNext(transaction).verifyComplete();
        StepVerifier.create(service.findByRange("user", from, to, 100L, null))
                .expectNext(transaction).verifyComplete();
        StepVerifier.create(service.findByInitiativeIdAndUserId("initiative", "user"))
                .expectNext(transaction).verifyComplete();

        verify(searchPort).findByIdTrxIssuer("issuer", "user", from, to, 100L, null);
        verify(searchPort).findByRange("user", from, to, 100L, null);
        verify(searchPort).findByInitiativeIdAndUserId("initiative", "user");
    }

    @Test
    void nonInvoicedSaveUsesSqlSynchronizationPort() {
        RewardTransaction transaction = RewardTransaction.builder()
                .id("transaction").status("AUTHORIZED").build();
        when(synchronizationPort.upsert(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(service.save(transaction))
                .expectNext(transaction)
                .verifyComplete();

        verify(synchronizationPort).upsert(transaction);
    }

    @Test
    void invoicedSaveAssignsTheTransactionToItsSqlBatch() {
        RewardTransaction transaction = invoicedTransaction("transaction");
        when(assignmentPort.assignInvoicedTransaction(
                eq(transaction), any(RewardBatch.class),
                anyInt())).thenReturn(Mono.just(transaction));

        StepVerifier.create(service.save(transaction))
                .expectNext(transaction)
                .verifyComplete();

        verify(assignmentPort).assignInvoicedTransaction(
                eq(transaction), argThat(batch ->
                        batch.getInitiativeId().equals("initiative")
                                && batch.getMerchantId().equals("merchant")
                                && batch.getPosType() == PosType.PHYSICAL
                                && batch.getMonth().equals("2026-02")),
                eq(service.computeSamplingKey("transaction")));
    }

    @Test
    void singleInvoiceAssignmentEnrichesMissingFieldsThenAssigns() {
        RewardTransaction transaction = invoicedTransaction("transaction");
        transaction.setInvoiceUploadDate(null);
        transaction.setFranchiseName(null);
        transaction.setPointOfSaleType(null);
        transaction.setBusinessName(null);
        when(assignmentPort.findInvoicedTransactionWithoutBatch("transaction"))
                .thenReturn(Mono.just(transaction));
        when(merchantRestClient.getPointOfSale("merchant", "pos")).thenReturn(Mono.just(
                PointOfSaleDTO.builder().type(PointOfSaleTypeEnum.PHYSICAL)
                        .franchiseName("franchise").businessName("business").build()));
        when(assignmentPort.assignInvoicedTransaction(
                eq(transaction), any(),
                anyInt())).thenReturn(Mono.just(transaction));

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 1, false, "transaction"))
                .verifyComplete();

        assertEquals(
                transaction.getTrxChargeDate(), transaction.getInvoiceUploadDate());
        assertEquals("franchise", transaction.getFranchiseName());
        assertEquals(PosType.PHYSICAL, transaction.getPointOfSaleType());
        verify(assignmentPort).assignInvoicedTransaction(
                eq(transaction), any(),
                anyInt());
    }

    @Test
    void missingExplicitInvoiceAssignmentReturnsNotFound() {
        when(assignmentPort.findInvoicedTransactionWithoutBatch("missing")).thenReturn(Mono.empty());

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 1, false, "missing"))
                .expectError(it.gov.pagopa.common.web.exception.ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void batchAssignmentContinuesAfterAnIndividualSqlAssignmentFailure() {
        RewardTransaction failing = invoicedTransaction("failing");
        RewardTransaction succeeding = invoicedTransaction("succeeding");
        when(assignmentPort.findInvoicedTransactionsWithoutBatch(10))
                .thenReturn(Flux.just(failing, succeeding), Flux.empty());
        when(merchantRestClient.getPointOfSale("merchant", "pos")).thenReturn(Mono.just(
                PointOfSaleDTO.builder().type(PointOfSaleTypeEnum.PHYSICAL)
                        .franchiseName("franchise").businessName("business").build()));
        when(assignmentPort.assignInvoicedTransaction(
                eq(failing), any(),
                anyInt())).thenReturn(Mono.error(new IllegalStateException("failure")));
        when(assignmentPort.assignInvoicedTransaction(
                eq(succeeding), any(),
                anyInt())).thenReturn(Mono.just(succeeding));

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 1, true, null))
                .verifyComplete();

        verify(assignmentPort).assignInvoicedTransaction(
                eq(succeeding), any(),
                anyInt());
    }

    @Test
    void samplingKeyIsDeterministicAndSeedSpecific() {
        RewardTransactionServiceImpl otherSeed = new RewardTransactionServiceImpl(
                searchPort, synchronizationPort, assignmentPort, merchantRestClient, 456);

        assertEquals(
                service.computeSamplingKey("id"), service.computeSamplingKey("id"));
        assertNotEquals(
                service.computeSamplingKey("id"), otherSeed.computeSamplingKey("id"));
    }

    @Test
    void repeatedAssignmentRunsEachRequestedChunkAndStopsOnEmptyChunks() {
        when(assignmentPort.findInvoicedTransactionsWithoutBatch(10))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 2, false, null))
                .verifyComplete();

        verify(assignmentPort, times(2)).findInvoicedTransactionsWithoutBatch(10);
    }

    @Test
    void assignmentPreservesAlreadyEnrichedTransactionFields() {
        RewardTransaction transaction = invoicedTransaction("transaction");
        when(assignmentPort.findInvoicedTransactionWithoutBatch("transaction"))
                .thenReturn(Mono.just(transaction));
        when(merchantRestClient.getPointOfSale("merchant", "pos")).thenReturn(Mono.just(
                PointOfSaleDTO.builder().type(PointOfSaleTypeEnum.ONLINE)
                        .franchiseName("different").businessName("different").build()));
        when(assignmentPort.assignInvoicedTransaction(
                any(), any(),
                anyInt())).thenReturn(Mono.just(transaction));

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 1, false, "transaction"))
                .verifyComplete();

        assertEquals("franchise", transaction.getFranchiseName());
        assertEquals(PosType.PHYSICAL, transaction.getPointOfSaleType());
        assertEquals("business", transaction.getBusinessName());
    }

    @Test
    void assignmentPropagatesMerchantLookupFailureForAnExplicitTransaction() {
        RewardTransaction transaction = invoicedTransaction("transaction");
        when(assignmentPort.findInvoicedTransactionWithoutBatch("transaction"))
                .thenReturn(Mono.just(transaction));
        IllegalStateException failure = new IllegalStateException("merchant unavailable");
        when(merchantRestClient.getPointOfSale("merchant", "pos")).thenReturn(Mono.error(failure));

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 1, false, "transaction"))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }

    @Test
    void repeatedAssignmentProcessesRowsAndSuppressesPerRowFailure() {
        RewardTransaction failing = invoicedTransaction("failing");
        RewardTransaction succeeding = invoicedTransaction("succeeding");
        when(assignmentPort.findInvoicedTransactionsWithoutBatch(10))
                .thenReturn(Flux.just(failing, succeeding));
        when(merchantRestClient.getPointOfSale("merchant", "pos")).thenReturn(Mono.just(
                PointOfSaleDTO.builder().type(PointOfSaleTypeEnum.PHYSICAL)
                        .franchiseName("franchise").businessName("business").build()));
        when(assignmentPort.assignInvoicedTransaction(
                eq(failing), any(),
                anyInt())).thenReturn(Mono.error(new IllegalStateException("failure")));
        when(assignmentPort.assignInvoicedTransaction(
                eq(succeeding), any(),
                anyInt())).thenReturn(Mono.just(succeeding));

        StepVerifier.create(service.assignInvoicedTransactionsToBatches(10, 1, false, null))
                .verifyComplete();

        verify(assignmentPort).assignInvoicedTransaction(
                eq(succeeding), any(),
                anyInt());
    }

    private static RewardTransaction invoicedTransaction(String id) {
        return RewardTransaction.builder()
                .id(id)
                .status(SyncTrxStatus.INVOICED.name())
                .initiatives(List.of("initiative"))
                .merchantId("merchant")
                .pointOfSaleId("pos")
                .pointOfSaleType(PosType.PHYSICAL)
                .businessName("business")
                .franchiseName("franchise")
                .trxChargeDate(YearMonth.of(2026, Month.FEBRUARY).atDay(10).atStartOfDay())
                .invoiceUploadDate(YearMonth.of(2026, Month.FEBRUARY).atDay(11).atStartOfDay())
                .build();
    }
}
