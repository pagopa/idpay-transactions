package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoiceTransactionLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSearchPort;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

    @Mock private UserRestClient userRestClient;
    @Mock private RewardTransactionSearchPort searchPort;
    @Mock private InvoiceTransactionLookupPort invoiceLookupPort;
    @Mock private RewardBatchTransactionReadPort batchReadPort;
    @Mock private InvoiceStorageClient invoiceStorageClient;
    private PointOfSaleTransactionServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new PointOfSaleTransactionServiceImpl(
                userRestClient, searchPort, invoiceLookupPort, batchReadPort, invoiceStorageClient);
    }

    @Test
    void searchesPointOfSaleThroughSqlSearchPortAndCountsTheSameScope() {
        TrxFiltersDTO filters = new TrxFiltersDTO();
        RewardTransaction transaction = RewardTransaction.builder().id("transaction").build();
        when(searchPort.findPointOfSaleTransactions(
                any(), org.mockito.ArgumentMatchers.eq("pos"), org.mockito.ArgumentMatchers.isNull(),
                org.mockito.ArgumentMatchers.eq("gtin"), org.mockito.ArgumentMatchers.eq(false), any()))
                .thenReturn(Flux.just(transaction));
        when(searchPort.countPointOfSaleTransactions(
                any(), org.mockito.ArgumentMatchers.eq("pos"), org.mockito.ArgumentMatchers.eq("gtin"),
                org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.eq(false)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getPointOfSaleTransactions(
                        "merchant", "initiative", "pos", "gtin", filters, PageRequest.of(0, 10)))
                .assertNext(page -> {
                    assertEquals(1, page.getTotalElements());
                    assertEquals(transaction, page.getContent().getFirst());
                })
                .verifyComplete();

        verify(searchPort).findPointOfSaleTransactions(
                filters, "pos", null, "gtin", false, PageRequest.of(0, 10));
        verify(searchPort).countPointOfSaleTransactions(filters, "pos", "gtin", null, false);
    }

    @Test
    void fiscalCodeSearchResolvesUserBeforeUsingSqlSearchPort() {
        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setFiscalCode("fiscal-code");
        when(userRestClient.retrieveFiscalCodeInfo("fiscal-code"))
                .thenReturn(Mono.just(new FiscalCodeInfoPDV("user")));
        when(searchPort.findPointOfSaleTransactions(
                any(), any(), any(), any(), org.mockito.ArgumentMatchers.anyBoolean(), any()))
                .thenReturn(Flux.empty());
        when(searchPort.countPointOfSaleTransactions(
                any(), any(), any(), any(), org.mockito.ArgumentMatchers.anyBoolean()))
                .thenReturn(Mono.just(0L));

        StepVerifier.create(service.getPointOfSaleTransactions(
                        "merchant", "initiative", "pos", null, filters, PageRequest.of(0, 10)))
                .expectNextCount(1).verifyComplete();

        verify(userRestClient).retrieveFiscalCodeInfo("fiscal-code");
        verify(searchPort).findPointOfSaleTransactions(
                filters, "pos", "user", null, false, PageRequest.of(0, 10));
    }

    @Test
    void returnsDistinctPointOfSaleRowsFromSqlBatchReadPort() {
        FranchisePointOfSaleDTO pointOfSale = new FranchisePointOfSaleDTO();
        when(batchReadPort.findDistinctFranchiseAndPosByRewardBatchId("batch", "merchant"))
                .thenReturn(Flux.just(pointOfSale));

        StepVerifier.create(service.getDistinctFranchiseAndPosByRewardBatchId("batch", "merchant"))
                .expectNext(List.of(pointOfSale))
                .verifyComplete();
    }

    @ParameterizedTest
    @CsvSource({
            "INVOICED, invoice, invoice.pdf",
            "REWARDED, invoice, invoice.pdf",
            "REFUNDED, creditNote, credit-note.pdf"
    })
    void downloadsRetainedInvoiceDocumentsFromTheStatusSpecificStoragePath(
            String status, String folder, String filename) {
        var transaction = RewardTransaction.builder().status(status);
        if (SyncTrxStatus.REFUNDED.name().equals(status)) {
            transaction.creditNoteData(InvoiceData.builder().filename(filename).build());
        } else {
            transaction.invoiceData(InvoiceData.builder().filename(filename).build());
        }
        when(invoiceLookupPort.findInvoiceTransaction("merchant", "transaction"))
                .thenReturn(Mono.just(transaction.build()));
        when(invoiceStorageClient.getFileSignedUrl(any())).thenReturn("signed-url");

        StepVerifier.create(service.downloadTransactionInvoice("merchant", "pos", "transaction"))
                .assertNext(response -> assertEquals("signed-url", response.getInvoiceUrl()))
                .verifyComplete();

        verify(invoiceStorageClient).getFileSignedUrl(
                "invoices/merchant/merchant/pos/pos/transaction/transaction/%s/%s"
                        .formatted(folder, filename));
    }

    @Test
    void retainedInvoiceDownloadRejectsMissingTransactionAndInvalidDocuments() {
        when(invoiceLookupPort.findInvoiceTransaction("merchant", "missing")).thenReturn(Mono.empty());
        StepVerifier.create(service.downloadTransactionInvoice("merchant", "pos", "missing"))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        RewardTransaction missingDocument = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name()).build();
        when(invoiceLookupPort.findInvoiceTransaction("merchant", "invalid"))
                .thenReturn(Mono.just(missingDocument));
        StepVerifier.create(service.downloadTransactionInvoice("merchant", "pos", "invalid"))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        RewardTransaction unsupported = RewardTransaction.builder().status("AUTHORIZED").build();
        when(invoiceLookupPort.findInvoiceTransaction("merchant", "unsupported"))
                .thenReturn(Mono.just(unsupported));
        StepVerifier.create(service.downloadTransactionInvoice("merchant", "pos", "unsupported"))
                .expectError(ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void propagatesFiscalCodeLookupFailureWithoutRunningTheSqlSearch() {
        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setFiscalCode("fiscal-code");
        IllegalStateException failure = new IllegalStateException("user lookup failed");
        when(userRestClient.retrieveFiscalCodeInfo("fiscal-code")).thenReturn(Mono.error(failure));

        StepVerifier.create(service.getPointOfSaleTransactions(
                        "merchant", "initiative", "pos", null, filters, PageRequest.of(0, 10)))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }

    @Test
    void propagatesBatchPointOfSaleReadFailure() {
        IllegalStateException failure = new IllegalStateException("database unavailable");
        when(batchReadPort.findDistinctFranchiseAndPosByRewardBatchId("batch", "merchant"))
                .thenReturn(Flux.error(failure));

        StepVerifier.create(service.getDistinctFranchiseAndPosByRewardBatchId("batch", "merchant"))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }
}
