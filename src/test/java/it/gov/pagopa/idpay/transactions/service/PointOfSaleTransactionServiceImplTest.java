package it.gov.pagopa.idpay.transactions.service;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_ON_GET_FILE_URL_REQUEST;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.ClientException;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

    @Mock
    private UserRestClient userRestClient;

    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Mock
    private InvoiceStorageClient invoiceStorageClient;

    @Mock
    private RewardBatchService rewardBatchService;

    @Mock
    private RewardBatchRepository rewardBatchRepository;

    private PointOfSaleTransactionService pointOfSaleTransactionService;

    private final Pageable pageable = PageRequest.of(0, 10);

    private static final String MERCHANT_ID = "MERCHANTID1";
    private static final String INITIATIVE_ID = "INITIATIVEID1";
    private static final String POINT_OF_SALE_ID = "POINTOFSALEID1";
    private static final String FISCAL_CODE = "FISCALCODE1";
    private static final String USER_ID = "USERID1";
    private static final String STATUS = "REWARDED";
    private static final String TRX_ID = "TRX_ID";
    private static final String DOC_NUMBER = "DOC123";
    private static final String NEW_DOC_NUMBER = "new_DOC123";
    private static final String OLD_FILENAME = "old_invoice.pdf";
    private static final String NEW_FILENAME = "new_invoice.pdf";

    @BeforeEach
    void setUp() {
        Mockito.reset(rewardTransactionRepository, invoiceStorageClient, userRestClient, rewardBatchService, rewardBatchRepository);
        pointOfSaleTransactionService = new PointOfSaleTransactionServiceImpl(
                userRestClient, rewardTransactionRepository, invoiceStorageClient, rewardBatchService, rewardBatchRepository);
    }

    @Test
    void getPointOfSaleTransactionsWithFiscalCode() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);

        when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE))
                .thenReturn(Mono.just(new FiscalCodeInfoPDV(USER_ID)));

        when(rewardTransactionRepository.findByFilterTrx(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                eq(USER_ID),
                eq(""),
                eq(false),
                eq(pageable)))
                .thenReturn(Flux.just(trx));

        when(rewardTransactionRepository.getCount(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                eq(""),
                eq(USER_ID),
                eq(false)))
                .thenReturn(Mono.just(1L));

        Mono<Page<RewardTransaction>> result = pointOfSaleTransactionService
                .getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, "",
                        FISCAL_CODE, STATUS, pageable);

        StepVerifier.create(result)
                .assertNext(page -> {
                    assertEquals(1, page.getTotalElements());
                    assertEquals(1, page.getContent().size());
                    assertEquals(trx.getIdTrxAcquirer(), page.getContent().get(0).getIdTrxAcquirer());
                })
                .verifyComplete();

        verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);
        verify(rewardTransactionRepository).findByFilterTrx(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                eq(USER_ID),
                eq(""),
                eq(false),
                eq(pageable)
        );
        verify(rewardTransactionRepository).getCount(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                eq(""),
                eq(USER_ID),
                eq(false)
        );
    }

    @Test
    void getPointOfSaleTransactionsWithoutFiscalCode() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(2);

        when(rewardTransactionRepository.findByFilterTrx(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                isNull(),
                isNull(),
                eq(false),
                eq(pageable)))
                .thenReturn(Flux.just(trx));

        when(rewardTransactionRepository.getCount(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                isNull(),
                isNull(),
                eq(false)))
                .thenReturn(Mono.just(1L));

        Mono<Page<RewardTransaction>> result = pointOfSaleTransactionService
                .getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID,
                        null, null, STATUS, pageable);

        StepVerifier.create(result)
                .assertNext(page -> {
                    assertEquals(1, page.getTotalElements());
                    assertEquals(1, page.getContent().size());
                    assertEquals(trx.getIdTrxAcquirer(), page.getContent().get(0).getIdTrxAcquirer());
                })
                .verifyComplete();

        verify(rewardTransactionRepository).findByFilterTrx(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                isNull(),
                isNull(),
                eq(false),
                eq(pageable)
        );
        verify(rewardTransactionRepository).getCount(
                any(TrxFiltersDTO.class),
                eq(POINT_OF_SALE_ID),
                isNull(),
                isNull(),
                eq(false)
        );
        verifyNoInteractions(userRestClient);
    }

    @Test
    void downloadTransactionInvoice_shouldThrow_whenInvoiceDataNull() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("INVOICED")
                .invoiceData(null)
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(
                        pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                                TRX_ID))
                .expectErrorMatches(throwable ->
                        throwable instanceof ClientExceptionNoBody
                                && ((ClientExceptionNoBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST
                                && TRANSACTION_MISSING_INVOICE.equals(throwable.getMessage()))
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void downloadTransactionInvoice_shouldThrow_whenInvoiceFilenameNull() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("REWARDED")
                .invoiceData(InvoiceData.builder().filename(null).build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(
                        pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                                TRX_ID))
                .expectErrorMatches(throwable ->
                        throwable instanceof ClientExceptionNoBody
                                && ((ClientExceptionNoBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST
                                && TRANSACTION_MISSING_INVOICE.equals(throwable.getMessage()))
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void downloadTransactionInvoice_shouldThrow_whenCreditNoteDataNull() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("REFUNDED")
                .creditNoteData(null)
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(
                        pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                                TRX_ID))
                .expectErrorMatches(throwable ->
                        throwable instanceof ClientExceptionNoBody
                                && ((ClientExceptionNoBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST
                                && TRANSACTION_MISSING_INVOICE.equals(throwable.getMessage()))
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void downloadTransactionInvoice_shouldReturnInvoiceUrl_whenStatusInvoiced() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("INVOICED")
                .invoiceData(InvoiceData.builder().filename("invoice.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        when(invoiceStorageClient.getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/invoice.pdf"))
                .thenReturn("tokenUrl");

        Mono<DownloadInvoiceResponseDTO> result =
                pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                        TRX_ID);

        StepVerifier.create(result)
                .assertNext(dto -> assertEquals("tokenUrl", dto.getInvoiceUrl()))
                .verifyComplete();

        verify(invoiceStorageClient).getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/invoice.pdf");
    }

    @Test
    void downloadTransactionInvoice_shouldReturnInvoiceUrl_whenStatusRewarded() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("REWARDED")
                .invoiceData(InvoiceData.builder().filename("rewarded.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        when(invoiceStorageClient.getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/rewarded.pdf"))
                .thenReturn("tokenUrl");

        Mono<DownloadInvoiceResponseDTO> result =
                pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                        TRX_ID);

        StepVerifier.create(result)
                .assertNext(dto -> assertEquals("tokenUrl", dto.getInvoiceUrl()))
                .verifyComplete();

        verify(invoiceStorageClient).getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/rewarded.pdf");
    }

    @Test
    void downloadTransactionInvoice_shouldReturnCreditNoteUrl_whenStatusRefunded() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("REFUNDED")
                .creditNoteData(InvoiceData.builder().filename("creditNote.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        when(invoiceStorageClient.getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/creditNote/creditNote.pdf"))
                .thenReturn("tokenUrl");

        Mono<DownloadInvoiceResponseDTO> result =
                pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                        TRX_ID);

        StepVerifier.create(result)
                .assertNext(dto -> assertEquals("tokenUrl", dto.getInvoiceUrl()))
                .verifyComplete();

        verify(invoiceStorageClient).getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/creditNote/creditNote.pdf");
    }

    @Test
    void downloadTransactionInvoice_shouldThrowMissingInvoice_whenTransactionNotFound() {
        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.empty());

        Mono<DownloadInvoiceResponseDTO> result =
                pointOfSaleTransactionService.downloadTransactionInvoice(MERCHANT_ID, POINT_OF_SALE_ID,
                        TRX_ID);

        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof ClientExceptionNoBody
                                && ((ClientExceptionNoBody) throwable).getHttpStatus()
                                .equals(HttpStatus.BAD_REQUEST)
                                && TRANSACTION_MISSING_INVOICE.equals(throwable.getMessage()))
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void downloadTransactionInvoice_shouldPropagateErrorOnFileUrlRecoveryError() {
        RewardTransaction rewardTransaction = RewardTransaction.builder()
                .status("INVOICED")
                .invoiceData(InvoiceData.builder().filename("filename").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, "AAAA"))
                .thenReturn(Mono.just(rewardTransaction));

        doThrow(new ClientException(HttpStatus.INTERNAL_SERVER_ERROR, ERROR_ON_GET_FILE_URL_REQUEST))
                .when(invoiceStorageClient).getFileSignedUrl(anyString());

        Mono<DownloadInvoiceResponseDTO> responseDTOMono =
                pointOfSaleTransactionService.downloadTransactionInvoice(
                        MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");

        StepVerifier.create(responseDTOMono)
                .expectErrorSatisfies(throwable -> {
                    assertInstanceOf(ClientException.class, throwable);
                    ClientException ex = (ClientException) throwable;
                    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, ex.getHttpStatus());
                    assertEquals(ERROR_ON_GET_FILE_URL_REQUEST, ex.getMessage());
                })
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");
        verify(invoiceStorageClient).getFileSignedUrl(anyString());
    }

    @Test
    void updateInvoiceTransaction_success() throws IOException {
        Path tempPath = Files.createTempFile("new_invoice", ".pdf");
        Files.write(tempPath, "test content".getBytes());

        FilePart filePart = createMockFilePart(tempPath.getFileName().toString(),
                MediaType.APPLICATION_PDF_VALUE);
        RewardTransaction trx =
                RewardTransaction.builder()
                        .id(TRX_ID)
                        .merchantId(MERCHANT_ID)
                        .pointOfSaleId(POINT_OF_SALE_ID)
                        .initiatives(List.of("1234"))
                        .status("INVOICED")
                        .invoiceData(InvoiceData.builder().filename(OLD_FILENAME).docNumber("OLD_DOC").build())
                        .rewardBatchId("OLD_BATCH_ID")
                        .pointOfSaleType(PosType.ONLINE)
                        .businessName("Test Business")
                        .rewards(Map.of("1234", Reward.builder()
                                .accruedRewardCents(1000L)
                                .build()))
                        .build();

        RewardBatch newBatch = new RewardBatch();
        newBatch.setId("NEW_BATCH_ID");
        newBatch.setStatus(RewardBatchStatus.CREATED);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(filePart.transferTo(any(Path.class))).thenAnswer(invocation -> {
            Path target = invocation.getArgument(0);
            Files.copy(tempPath, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return Mono.empty();
        });
        when(invoiceStorageClient.deleteFile(anyString())).thenReturn(null);
        when(invoiceStorageClient.upload(any(), anyString(), anyString())).thenReturn(null);
        when(rewardBatchService.findOrCreateBatch(eq(MERCHANT_ID), any(), anyString(), eq("Test Business")))
                .thenReturn(Mono.just(newBatch));
        when(rewardBatchService.decrementTotals(eq("OLD_BATCH_ID"), eq(1000L)))
                .thenReturn(Mono.empty());
        when(rewardBatchService.incrementTotals(eq("NEW_BATCH_ID"), eq(1000L)))
                .thenReturn(Mono.empty());
        when(rewardTransactionRepository.save(any(RewardTransaction.class))).thenReturn(Mono.just(trx));

        when(rewardBatchRepository.findRewardBatchById(anyString())).thenReturn(Mono.just(newBatch));

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
                TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
                .verifyComplete();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verify(invoiceStorageClient).deleteFile(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/old_invoice.pdf");
        verify(invoiceStorageClient).upload(any(), eq(
                        "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/"
                                + tempPath.getFileName().toString()),
                eq(MediaType.APPLICATION_PDF_VALUE));
        verify(rewardBatchService).findOrCreateBatch(eq(MERCHANT_ID), any(), anyString(), eq("Test Business"));
        verify(rewardBatchService).decrementTotals("OLD_BATCH_ID", 1000L);
        verify(rewardBatchService).incrementTotals("NEW_BATCH_ID", 1000L);
        verify(rewardTransactionRepository).save(any(RewardTransaction.class));

        Files.deleteIfExists(tempPath);
    }

    @Test
    void updateInvoiceTransaction_transactionNotFound() {
        FilePart filePart = createMockFilePart(NEW_FILENAME, MediaType.APPLICATION_PDF_VALUE);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.empty());

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
                TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
                .expectErrorMatches(throwable -> throwable instanceof ClientExceptionNoBody &&
                        ((ClientExceptionNoBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST &&
                        throwable.getMessage().equals(TRANSACTION_MISSING_INVOICE))
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void updateInvoiceTransaction_statusNotInvoiced() {
        FilePart filePart = createMockFilePart(NEW_FILENAME, MediaType.APPLICATION_PDF_VALUE);
        RewardTransaction trx = RewardTransaction.builder()
                .id(TRX_ID)
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .status("REWARDED")
                .invoiceData(InvoiceData.builder().filename(OLD_FILENAME).build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
                TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
                .expectErrorMatches(throwable -> throwable instanceof ClientExceptionNoBody &&
                        ((ClientExceptionNoBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST &&
                        throwable.getMessage().equals(TRANSACTION_MISSING_INVOICE))
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void updateInvoiceTransaction_merchantIdMismatch() {
        FilePart filePart = createMockFilePart(NEW_FILENAME, MediaType.APPLICATION_PDF_VALUE);
        RewardTransaction trx = RewardTransaction.builder()
                .id(TRX_ID)
                .merchantId("DIFFERENT_MERCHANT")
                .pointOfSaleId(POINT_OF_SALE_ID)
                .status("INVOICED")
                .invoiceData(InvoiceData.builder().filename(OLD_FILENAME).build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
                TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
                .expectErrorMatches(throwable -> throwable instanceof ClientException &&
                        ((ClientException) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST)
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void updateInvoiceTransaction_pointOfSaleIdMismatch() {
        FilePart filePart = createMockFilePart(NEW_FILENAME, MediaType.APPLICATION_PDF_VALUE);
        RewardTransaction trx = RewardTransaction.builder()
                .id(TRX_ID)
                .merchantId(MERCHANT_ID)
                .pointOfSaleId("DIFFERENT_POS")
                .status("INVOICED")
                .invoiceData(InvoiceData.builder().filename(OLD_FILENAME).build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
                TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
                .expectErrorMatches(throwable -> throwable instanceof ClientException &&
                        ((ClientException) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST)
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void updateInvoiceTransaction_invalidFileFormat() {
        FilePart filePart = createMockFilePart("invalid.txt", "text/plain");

        ClientExceptionWithBody ex = assertThrows(ClientExceptionWithBody.class,
                () -> pointOfSaleTransactionService.updateInvoiceTransaction(
                        TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER));

        assertEquals("File must be a PDF or XML", ex.getCause().getMessage());
    }

    @Test
    void updateInvoiceTransaction_nullFile() {
        ClientExceptionWithBody ex = assertThrows(ClientExceptionWithBody.class,
                () -> pointOfSaleTransactionService.updateInvoiceTransaction(
                        TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, null, DOC_NUMBER));

        assertEquals("File is required", ex.getCause().getMessage());
    }

    @Test
    void updateInvoiceTransaction_rewardBatchNotFound_shouldThrowTransactionNotFound() {
        FilePart filePart = createMockFilePart(NEW_FILENAME, MediaType.APPLICATION_PDF_VALUE);

        RewardTransaction trx = RewardTransaction.builder()
            .id(TRX_ID)
            .merchantId(MERCHANT_ID)
            .pointOfSaleId(POINT_OF_SALE_ID)
            .status("INVOICED")
            .invoiceData(InvoiceData.builder().filename(OLD_FILENAME).build())
            .rewardBatchId("BATCH_ID")
            .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
            .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("BATCH_ID"))
            .thenReturn(Mono.empty());

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
            TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
            .expectErrorMatches(throwable ->
                throwable instanceof ClientExceptionNoBody &&
                    ((ClientExceptionNoBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST &&
                    TRANSACTION_NOT_FOUND.equals(throwable.getMessage()))
            .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verify(rewardBatchRepository).findRewardBatchById("BATCH_ID");
        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void updateInvoiceTransaction_rewardBatchStatusNotCreated_shouldThrowRewardBatchAlreadySent() {
        FilePart filePart = createMockFilePart(NEW_FILENAME, MediaType.APPLICATION_PDF_VALUE);

        RewardTransaction trx = RewardTransaction.builder()
            .id(TRX_ID)
            .merchantId(MERCHANT_ID)
            .pointOfSaleId(POINT_OF_SALE_ID)
            .status("INVOICED")
            .invoiceData(InvoiceData.builder().filename(OLD_FILENAME).build())
            .rewardBatchId("BATCH_ID")
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH_ID");
        batch.setStatus(RewardBatchStatus.SENT); // stato diverso da CREATED

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID))
            .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("BATCH_ID"))
            .thenReturn(Mono.just(batch));

        Mono<Void> result = pointOfSaleTransactionService.updateInvoiceTransaction(
            TRX_ID, MERCHANT_ID, POINT_OF_SALE_ID, filePart, NEW_DOC_NUMBER);

        StepVerifier.create(result)
            .expectErrorMatches(throwable ->
                throwable instanceof ClientExceptionWithBody &&
                    ((ClientExceptionWithBody) throwable).getHttpStatus() == HttpStatus.BAD_REQUEST &&
                    throwable.getMessage().contains(
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_REWARD_BATCH_ALREADY_SENT
                    )
            )
            .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        verify(rewardBatchRepository).findRewardBatchById("BATCH_ID");
        verifyNoInteractions(invoiceStorageClient);
    }

    private FilePart createMockFilePart(String filename, String contentType) {
        FilePart filePart = mock(FilePart.class);
        HttpHeaders headers = new HttpHeaders();
        if (contentType != null) {
            headers.setContentType(MediaType.parseMediaType(contentType));
        }
        when(filePart.filename()).thenReturn(filename);
        when(filePart.headers()).thenReturn(headers);
        return filePart;
    }
}
