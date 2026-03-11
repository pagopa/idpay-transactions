package it.gov.pagopa.idpay.transactions.service;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionKafkaDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.notifier.TransactionNotifierService;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.invoiceLifecycle.InvoiceLifecyclePolicy;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.http.MediaType.APPLICATION_PDF_VALUE;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

    @Mock private UserRestClient userRestClient;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private InvoiceStorageClient invoiceStorageClient;
    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private TransactionErrorNotifierService transactionErrorNotifierService;
    @Mock private TransactionNotifierService transactionNotifierService;
    @Mock private RewardBatchService rewardBatchService;
    @Mock private InvoiceLifecyclePolicy invoiceLifeCyclePolicy;

    @InjectMocks private PointOfSaleTransactionServiceImpl service;

    private final Pageable pageable = PageRequest.of(0, 10);

    private static final String MERCHANT_ID = "MERCHANTID1";
    private static final String INITIATIVE_ID = "INITIATIVEID1";
    private static final String POS_ID = "POINTOFSALEID1";
    private static final String FISCAL_CODE = "FISCALCODE1";
    private static final String USER_ID = "USERID1";
    private static final String STATUS = "REWARDED";
    private static final String TRX_ID = "TRX_ID";
    private static final String DOC_NUMBER = "DOC123";

    private Path srcFile;

    @BeforeEach
    void setup() throws Exception {
        srcFile = Files.createTempFile("src-", ".pdf");
        Files.write(srcFile, "content".getBytes());
    }

    @AfterEach
    void cleanup() throws Exception {
        Files.deleteIfExists(srcFile);
    }

    @Test
    void getPointOfSaleTransactions_withFiscalCode_resolvesUserAndReturnsPage() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);

        when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE))
                .thenReturn(Mono.just(new FiscalCodeInfoPDV(USER_ID)));

        ArgumentCaptor<TrxFiltersDTO> filtersCaptor = ArgumentCaptor.forClass(TrxFiltersDTO.class);

        when(rewardTransactionRepository.findByFilterTrx(
                filtersCaptor.capture(),
                eq(POS_ID),
                eq(USER_ID),
                eq(""),
                eq(false),
                eq(pageable)))
                .thenReturn(Flux.just(trx));

        when(rewardTransactionRepository.getCount(
                any(TrxFiltersDTO.class),
                eq(POS_ID),
                eq(""),
                eq(USER_ID),
                eq(false)))
                .thenReturn(Mono.just(1L));

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setFiscalCode(FISCAL_CODE);
        filters.setStatus(STATUS);

        StepVerifier.create(service.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POS_ID, "", filters, pageable))
                .assertNext(page -> {
                    assertEquals(1, page.getTotalElements());
                    assertEquals(1, page.getContent().size());
                })
                .verifyComplete();

        verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);

        TrxFiltersDTO passedFilters = filtersCaptor.getValue();
        assertNotNull(passedFilters);
        assertEquals(MERCHANT_ID, passedFilters.getMerchantId());
        assertEquals(INITIATIVE_ID, passedFilters.getInitiativeId());
        assertEquals(FISCAL_CODE, passedFilters.getFiscalCode());
    }

    @Test
    void getPointOfSaleTransactions_withoutFiscalCode_doesNotCallUserService() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(2);

        when(rewardTransactionRepository.findByFilterTrx(
                any(TrxFiltersDTO.class),
                eq(POS_ID),
                isNull(),
                isNull(),
                eq(false),
                eq(pageable)))
                .thenReturn(Flux.just(trx));

        when(rewardTransactionRepository.getCount(
                any(TrxFiltersDTO.class),
                eq(POS_ID),
                isNull(),
                isNull(),
                eq(false)))
                .thenReturn(Mono.just(1L));

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setStatus(STATUS);

        StepVerifier.create(service.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POS_ID, null, filters, pageable))
                .assertNext(page -> assertEquals(1, page.getTotalElements()))
                .verifyComplete();

        verifyNoInteractions(userRestClient);
    }

    @Test
    void getPointOfSaleTransactions_withFiscalCode_userServiceError_propagates() {
        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setFiscalCode(FISCAL_CODE);

        when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE))
                .thenReturn(Mono.error(new RuntimeException("boom")));

        StepVerifier.create(service.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POS_ID, "", filters, pageable))
                .expectErrorMatches(e -> e instanceof RuntimeException && "boom".equals(e.getMessage()))
                .verify();

        verifyNoInteractions(rewardTransactionRepository);
    }

    @Test
    void downloadTransactionInvoice_invoiced_ok_usesInvoiceFolder() {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .invoiceData(InvoiceData.builder().filename("invoice.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(invoiceStorageClient.getFileSignedUrl(anyString())).thenReturn("tokenUrl");

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .assertNext(dto -> assertEquals("tokenUrl", dto.getInvoiceUrl()))
                .verifyComplete();

        verify(invoiceStorageClient).getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/invoice.pdf");
    }

    @Test
    void downloadTransactionInvoice_rewarded_ok_usesInvoiceFolder() {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.REWARDED.name())
                .invoiceData(InvoiceData.builder().filename("invoice.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(invoiceStorageClient.getFileSignedUrl(anyString())).thenReturn("tokenUrl");

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .assertNext(dto -> assertEquals("tokenUrl", dto.getInvoiceUrl()))
                .verifyComplete();
    }

    @Test
    void downloadTransactionInvoice_refunded_ok_usesCreditNoteFolder() {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.REFUNDED.name())
                .creditNoteData(InvoiceData.builder().filename("cn.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(invoiceStorageClient.getFileSignedUrl(anyString())).thenReturn("cnUrl");

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .assertNext(dto -> assertEquals("cnUrl", dto.getInvoiceUrl()))
                .verifyComplete();

        verify(invoiceStorageClient).getFileSignedUrl(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/creditNote/cn.pdf");
    }

    @Test
    void downloadTransactionInvoice_missingTransaction_throws() {
        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void downloadTransactionInvoice_invoiced_invoiceDataNull_throws() {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .invoiceData(null)
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .expectError(ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void downloadTransactionInvoice_refunded_creditNoteNull_throws() {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.REFUNDED.name())
                .creditNoteData(null)
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .expectError(ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void downloadTransactionInvoice_missingFilename_throws() {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .invoiceData(InvoiceData.builder().filename(null).build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void downloadTransactionInvoice_statusNotAllowed_throws() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("AUTHORIZED")
                .invoiceData(InvoiceData.builder().filename("x.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void addCreditNoteFile_success_uploadsToExpectedPath() {
        FilePart fp = filePartBackedBySrc("cn.pdf", true);

        @SuppressWarnings("unchecked")
        Response<BlockBlobItem> uploadResponse = (Response<BlockBlobItem>) mock(Response.class);
        when(invoiceStorageClient.upload(any(InputStream.class), anyString(), anyString()))
                .thenReturn(uploadResponse);

        StepVerifier.create(service.addCreditNoteFile(fp, MERCHANT_ID, POS_ID, TRX_ID))
                .verifyComplete();

        verify(invoiceStorageClient).upload(
                any(InputStream.class),
                eq("invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/creditNote/cn.pdf"),
                eq("application/pdf"));
    }

    @Test
    void addCreditNoteFile_transferToFails_propagatesRuntime() {
        FilePart fp = mockFilePart("cn.pdf", true);
        when(fp.transferTo(any(Path.class)))
                .thenReturn(Mono.error(new RuntimeException("transfer failed")));

        StepVerifier.create(service.addCreditNoteFile(fp, MERCHANT_ID, POS_ID, TRX_ID))
                .expectErrorMatches(e -> e instanceof RuntimeException && "transfer failed".equals(e.getMessage()))
                .verify();

        verify(invoiceStorageClient, never()).upload(any(), anyString(), anyString());
    }

    @Test
    void replaceInvoiceFile_ok_deletesOldAndUploadsNew() {
        FilePart fp = filePartBackedBySrc("new.pdf", true);
        InvoiceData oldInvoice = InvoiceData.builder().filename("old.pdf").build();

        @SuppressWarnings("unchecked")
        Response<BlockBlobItem> uploadResponse = (Response<BlockBlobItem>) mock(Response.class);
        when(invoiceStorageClient.upload(any(InputStream.class), anyString(), anyString()))
                .thenReturn(uploadResponse);

        Mono<Void> result = ReflectionTestUtils.invokeMethod(
                service, "replaceInvoiceFile", fp, oldInvoice, MERCHANT_ID, POS_ID, TRX_ID);

        StepVerifier.create(result).verifyComplete();

        verify(invoiceStorageClient).deleteFile(
                "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/old.pdf");

        verify(invoiceStorageClient).upload(
                any(InputStream.class),
                eq("invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/invoice/new.pdf"),
                eq("application/pdf"));
    }

    @Test
    void updateInvoiceTransaction_transactionNotFound_throws() {
        FilePart fp = mockFilePart("invoice.pdf", true);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verify(rewardTransactionRepository).findTransaction(MERCHANT_ID, TRX_ID);
    }

    @Test
    void updateInvoiceTransaction_missingBatchId_throws() {
        FilePart fp = mockFilePart("invoice.pdf", true);

        RewardTransaction trx = new RewardTransaction();
        trx.setRewardBatchId(null);
        trx.setMerchantId(MERCHANT_ID);
        trx.setPointOfSaleId(POS_ID);
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verify(rewardBatchRepository, never()).findRewardBatchById(anyString());
    }

    @Test
    void updateInvoiceTransaction_rewardBatchNotFound_throws() {
        FilePart fp = mockFilePart("invoice.pdf", true);

        RewardTransaction trx = new RewardTransaction();
        trx.setRewardBatchId("B404");
        trx.setMerchantId(MERCHANT_ID);
        trx.setPointOfSaleId(POS_ID);
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("B404"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void updateInvoiceTransaction_createdBatch_updatesInvoice_doesNotMoveCounters() {
        FilePart fp = filePartBackedBySrc("invoice.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId("B1");
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setMerchantId(MERCHANT_ID);
        batch.setStatus(RewardBatchStatus.CREATED);
        batch.setMonth("2024-01");

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(invoiceLifeCyclePolicy).validate(trx, batch);
        verify(rewardTransactionRepository, times(1)).save(any());
        verify(rewardBatchRepository, never()).updateTotals(anyString(), any());
        verifyNoInteractions(rewardBatchService);
    }

    @Test
    void updateInvoiceTransaction_createdBatch_updatesInvoice_only_noCountersMoves() {
        FilePart fp = filePartBackedBySrc("invoice.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId("B1");
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setMerchantId(MERCHANT_ID);
        batch.setStatus(RewardBatchStatus.CREATED);
        batch.setMonth("2024-01");

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(invoiceLifeCyclePolicy).validate(trx, batch);
        verify(rewardBatchRepository, never()).updateTotals(anyString(), any());
        verify(rewardBatchService, never()).findOrCreateBatch(anyString(), any(), anyString(), anyString());
        verify(rewardTransactionRepository, times(1)).save(any());
    }

    @Test
    void updateInvoiceTransaction_evaluatingBatch_wasConsultable_movesAndUpdatesBothBatches() {
        FilePart fp = filePartBackedBySrc("invoice.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId("OLD");
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        trx.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(123L).build()));

        RewardBatch oldBatch = new RewardBatch();
        oldBatch.setId("OLD");
        oldBatch.setMerchantId(MERCHANT_ID);
        oldBatch.setStatus(RewardBatchStatus.EVALUATING);
        oldBatch.setMonth("2024-01");

        RewardBatch newBatch = new RewardBatch();
        newBatch.setId("NEW");
        newBatch.setMerchantId(MERCHANT_ID);
        newBatch.setStatus(RewardBatchStatus.CREATED);
        newBatch.setMonth(YearMonth.now().toString());

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("OLD"))
                .thenReturn(Mono.just(oldBatch));
        when(invoiceLifeCyclePolicy.validate(trx, oldBatch))
                .thenReturn(Mono.just(trx));

        ArgumentCaptor<RewardTransaction> trxCaptor = ArgumentCaptor.forClass(RewardTransaction.class);
        when(rewardTransactionRepository.save(trxCaptor.capture()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        when(rewardBatchService.findOrCreateBatch(eq(MERCHANT_ID), eq(PosType.PHYSICAL), anyString(), eq("Biz")))
                .thenReturn(Mono.just(newBatch));

        when(rewardBatchRepository.updateTotals(eq("OLD"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(oldBatch));
        when(rewardBatchRepository.updateTotals(eq("NEW"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(newBatch));

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(invoiceLifeCyclePolicy).validate(trx, oldBatch);

        verify(rewardBatchRepository).updateTotals(eq("OLD"), argThat(c ->
                Long.valueOf(-1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(0L).equals(c.getTrxElaborated())
        ));

        verify(rewardBatchRepository).updateTotals(eq("NEW"), argThat(c ->
                Long.valueOf(123L).equals(c.getInitialAmountCents()) &&
                        Long.valueOf(1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(1L).equals(c.getTrxSuspended()) &&
                        Long.valueOf(123L).equals(c.getSuspendedAmountCents()) &&
                        Long.valueOf(1L).equals(c.getTrxElaborated())
        ));

        assertTrue(trxCaptor.getAllValues().stream().anyMatch(t ->
                RewardBatchTrxStatus.SUSPENDED.equals(t.getRewardBatchTrxStatus()) &&
                        "NEW".equals(t.getRewardBatchId()) &&
                        SyncTrxStatus.INVOICED.name().equals(t.getStatus())
        ));

        verify(rewardTransactionRepository, times(2)).save(any());
    }

    @Test
    void updateInvoiceTransaction_evaluatingBatch_wasSuspended_updatesOldAndNewCounters() {
        FilePart fp = filePartBackedBySrc("invoice.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId("OLD");
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
        trx.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(123L).build()));

        RewardBatch oldBatch = new RewardBatch();
        oldBatch.setId("OLD");
        oldBatch.setMerchantId(MERCHANT_ID);
        oldBatch.setStatus(RewardBatchStatus.EVALUATING);
        oldBatch.setMonth("2024-01");

        RewardBatch newBatch = new RewardBatch();
        newBatch.setId("NEW");
        newBatch.setMerchantId(MERCHANT_ID);
        newBatch.setStatus(RewardBatchStatus.CREATED);
        newBatch.setMonth(YearMonth.now().toString());

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("OLD"))
                .thenReturn(Mono.just(oldBatch));
        when(invoiceLifeCyclePolicy.validate(trx, oldBatch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        when(rewardBatchService.findOrCreateBatch(eq(MERCHANT_ID), eq(PosType.PHYSICAL), anyString(), eq("Biz")))
                .thenReturn(Mono.just(newBatch));

        when(rewardBatchRepository.updateTotals(eq("OLD"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(oldBatch));
        when(rewardBatchRepository.updateTotals(eq("NEW"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(newBatch));

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(invoiceLifeCyclePolicy).validate(trx, oldBatch);

        verify(rewardBatchRepository).updateTotals(eq("OLD"), argThat(c ->
                Long.valueOf(-1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(-1L).equals(c.getTrxElaborated())
        ));

        verify(rewardBatchRepository).updateTotals(eq("NEW"), argThat(c ->
                Long.valueOf(123L).equals(c.getInitialAmountCents()) &&
                        Long.valueOf(1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(1L).equals(c.getTrxSuspended()) &&
                        Long.valueOf(123L).equals(c.getSuspendedAmountCents()) &&
                        Long.valueOf(1L).equals(c.getTrxElaborated())
        ));
    }

    @Test
    void updateInvoiceTransaction_evaluatingBatch_wasRejected_updatesOldAndNewCounters() {
        FilePart fp = filePartBackedBySrc("invoice.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId("OLD");
        trx.setInvoiceData(InvoiceData.builder().filename("old.pdf").build());
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);
        trx.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(123L).build()));

        RewardBatch oldBatch = new RewardBatch();
        oldBatch.setId("OLD");
        oldBatch.setMerchantId(MERCHANT_ID);
        oldBatch.setStatus(RewardBatchStatus.APPROVED);
        oldBatch.setMonth("2024-01");

        RewardBatch newBatch = new RewardBatch();
        newBatch.setId("NEW");
        newBatch.setMerchantId(MERCHANT_ID);
        newBatch.setStatus(RewardBatchStatus.CREATED);
        newBatch.setMonth(YearMonth.now().toString());

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("OLD"))
                .thenReturn(Mono.just(oldBatch));
        when(invoiceLifeCyclePolicy.validate(trx, oldBatch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        when(rewardBatchService.findOrCreateBatch(eq(MERCHANT_ID), eq(PosType.PHYSICAL), anyString(), eq("Biz")))
                .thenReturn(Mono.just(newBatch));

        when(rewardBatchRepository.updateTotals(eq("OLD"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(oldBatch));
        when(rewardBatchRepository.updateTotals(eq("NEW"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(newBatch));

        StepVerifier.create(service.updateInvoiceTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(invoiceLifeCyclePolicy).validate(trx, oldBatch);

        verify(rewardBatchRepository).updateTotals(eq("OLD"), argThat(c ->
                Long.valueOf(-1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(-1L).equals(c.getTrxElaborated())
        ));

        verify(rewardBatchRepository).updateTotals(eq("NEW"), argThat(c ->
                Long.valueOf(123L).equals(c.getInitialAmountCents()) &&
                        Long.valueOf(1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(1L).equals(c.getTrxSuspended()) &&
                        Long.valueOf(123L).equals(c.getSuspendedAmountCents()) &&
                        Long.valueOf(1L).equals(c.getTrxElaborated())
        ));
    }

    @Test
    void reversalTransaction_missingTransaction_throws400() {
        FilePart fp = mockFilePart("credit-note.pdf", true);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void reversalTransaction_withBatch_policyRejects_throws() {
        FilePart fp = mockFilePart("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.AUTHORIZED.toString());
        trx.setRewardBatchId("B1");

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.CREATED);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "CODE", "not allowed")));

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verify(rewardTransactionRepository, never()).save(any());
        verify(invoiceStorageClient, never()).upload(any(), anyString(), anyString());
    }

    @Test
    void reversalTransaction_withBatch_rewardBatchNotFound_throws() {
        FilePart fp = mockFilePart("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId("B404");

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B404"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verify(invoiceStorageClient, never()).upload(any(), anyString(), anyString());
        verify(rewardTransactionRepository, never()).save(any());
    }

    @Test
    void reversalTransaction_noBatch_throwsClientExceptionWithBody() {
        FilePart fp = mockFilePart("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId(null);
        trx.setRewardBatchTrxStatus(null);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById((String) null))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectErrorMatches(ex ->
                        ex instanceof ClientExceptionWithBody ceb &&
                                ceb.getHttpStatus() == HttpStatus.NOT_FOUND)
                .verify();

        verify(rewardBatchRepository, never()).updateTotals(anyString(), any());
        verify(rewardTransactionRepository, never()).save(any());
        verify(transactionNotifierService, never()).notify(any(RewardTransactionKafkaDTO.class), any());
    }

    @Test
    void reversalTransaction_success_withBatch_consultable_updatesTotalsAndNotifies() {
        FilePart fp = filePartBackedBySrc("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId("B1");
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.CREATED);

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.updateTotals(eq("B1"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(batch));
        when(transactionNotifierService.notify(any(), any()))
                .thenReturn(true);

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(eq("B1"), argThat(c ->
                Long.valueOf(-1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(-123L).equals(c.getInitialAmountCents())
        ));
    }

    @Test
    void reversalTransaction_success_withBatch_wasSuspended_updatesExtraCounters() {
        FilePart fp = filePartBackedBySrc("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId("B1");
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.CREATED);

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.updateTotals(eq("B1"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(batch));
        when(transactionNotifierService.notify(any(), any()))
                .thenReturn(true);

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(eq("B1"), argThat(c ->
                Long.valueOf(-1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(-123L).equals(c.getInitialAmountCents()) &&
                        Long.valueOf(-123L).equals(c.getSuspendedAmountCents()) &&
                        Long.valueOf(-1L).equals(c.getTrxSuspended()) &&
                        Long.valueOf(-1L).equals(c.getTrxElaborated())
        ));
    }

    @Test
    void reversalTransaction_success_withBatch_wasRejected_updatesRejectedCounters() {
        FilePart fp = filePartBackedBySrc("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.REWARDED.toString());
        trx.setRewardBatchId("B1");
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.APPROVED);

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.updateTotals(eq("B1"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(batch));
        when(transactionNotifierService.notify(any(), any()))
                .thenReturn(true);

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(eq("B1"), argThat(c ->
                Long.valueOf(-1L).equals(c.getNumberOfTransactions()) &&
                        Long.valueOf(-123L).equals(c.getInitialAmountCents()) &&
                        Long.valueOf(-1L).equals(c.getTrxRejected()) &&
                        Long.valueOf(-1L).equals(c.getTrxElaborated())
        ));
    }

    @Test
    void reversalTransaction_uploadRuntimeException_isMappedTo500() {
        FilePart fp = mockFilePart("credit-note.pdf", true);
        when(fp.transferTo(any(Path.class))).thenReturn(Mono.error(new RuntimeException("boom")));

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId("B1");

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.CREATED);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verify(rewardTransactionRepository, never()).save(any());
    }

    @Test
    void reversalTransaction_notifyFalse_triggersErrorNotifierAndPropagates() {
        FilePart fp = filePartBackedBySrc("credit-note.pdf", true);

        RewardTransaction trx = baseTransaction();
        trx.setId(TRX_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId("B1");
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.CREATED);

        @SuppressWarnings("unchecked")
        Message<RewardTransactionKafkaDTO> message = (Message<RewardTransactionKafkaDTO>) mock(Message.class);

        stubUploadOk();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, TRX_ID))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById("B1"))
                .thenReturn(Mono.just(batch));
        when(invoiceLifeCyclePolicy.validate(trx, batch))
                .thenReturn(Mono.just(trx));
        when(rewardTransactionRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.updateTotals(eq("B1"), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(batch));
        when(transactionNotifierService.notify(any(), any()))
                .thenReturn(false);
        when(transactionNotifierService.buildMessage(any(), any()))
                .thenReturn(message);

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, fp, DOC_NUMBER, invoiceLifeCyclePolicy))
                .expectError(IllegalStateException.class)
                .verify();

        verify(transactionErrorNotifierService).notifyTransactionOutcome(
                eq(message),
                contains("REVERSAL_INVOICED_TRANSACTION_REQUEST"),
                eq(true),
                any(Throwable.class));
    }

    @Test
    void getDistinctFranchiseAndPosByRewardBatchId_returnsList() {
        String rewardBatchId = "BATCH1";

        FranchisePointOfSaleDTO dto1 = new FranchisePointOfSaleDTO();
        dto1.setFranchiseName("FRANCHISE_1");
        dto1.setPointOfSaleId("POS_1");

        FranchisePointOfSaleDTO dto2 = new FranchisePointOfSaleDTO();
        dto2.setFranchiseName("FRANCHISE_2");
        dto2.setPointOfSaleId("POS_2");

        when(rewardTransactionRepository.findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId))
                .thenReturn(Flux.just(dto1, dto2));

        StepVerifier.create(service.getDistinctFranchiseAndPosByRewardBatchId(rewardBatchId))
                .assertNext(list -> {
                    assertEquals(2, list.size());
                    assertEquals("FRANCHISE_1", list.get(0).getFranchiseName());
                    assertEquals("POS_1", list.get(0).getPointOfSaleId());
                })
                .verifyComplete();
    }

    @Test
    void getDistinctFranchiseAndPosByRewardBatchId_propagatesError() {
        String rewardBatchId = "BATCH_ERROR";

        when(rewardTransactionRepository.findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId))
                .thenReturn(Flux.error(new RuntimeException("mongo failure")));

        StepVerifier.create(service.getDistinctFranchiseAndPosByRewardBatchId(rewardBatchId))
                .expectErrorMatches(e -> e instanceof RuntimeException && "mongo failure".equals(e.getMessage()))
                .verify();
    }

    @Test
    void getDistinctFranchiseAndPosByRewardBatchId_returnsEmptyList() {
        String rewardBatchId = "BATCH_EMPTY";

        when(rewardTransactionRepository.findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.getDistinctFranchiseAndPosByRewardBatchId(rewardBatchId))
                .assertNext(List::isEmpty)
                .verifyComplete();
    }

    private void stubUploadOk() {
        @SuppressWarnings("unchecked")
        Response<BlockBlobItem> uploadResponse = (Response<BlockBlobItem>) mock(Response.class);
        when(invoiceStorageClient.upload(any(InputStream.class), anyString(), anyString()))
                .thenReturn(uploadResponse);
    }

    private RewardTransaction baseTransaction() {
        Reward reward = new Reward();
        reward.setAccruedRewardCents(123L);

        RewardTransaction trx = new RewardTransaction();
        trx.setUpdateDate(LocalDateTime.now().minusDays(1));
        trx.setInitiatives(List.of(INITIATIVE_ID));
        trx.setRewards(Map.of(INITIATIVE_ID, reward));
        trx.setPointOfSaleType(PosType.PHYSICAL);
        trx.setBusinessName("Biz");
        trx.setUserId(USER_ID);
        trx.setMerchantId(MERCHANT_ID);
        trx.setPointOfSaleId(POS_ID);
        trx.setInitiativeId(INITIATIVE_ID);
        return trx;
    }

    private FilePart mockFilePart(String filename, boolean withPdfContentType) {
        FilePart filePart = mock(FilePart.class);

        lenient().when(filePart.filename()).thenReturn(filename);

        HttpHeaders headers = new HttpHeaders();
        if (withPdfContentType) {
            headers.setContentType(MediaType.parseMediaType(APPLICATION_PDF_VALUE));
        }
        lenient().when(filePart.headers()).thenReturn(headers);

        return filePart;
    }

    private FilePart filePartBackedBySrc(String filename, boolean withPdfContentType) {
        FilePart fp = mockFilePart(filename, withPdfContentType);
        when(fp.transferTo(any(Path.class)))
                .thenAnswer(inv -> {
                    Path target = inv.getArgument(0);
                    Files.copy(srcFile, target, StandardCopyOption.REPLACE_EXISTING);
                    return Mono.empty();
                });
        return fp;
    }
}