package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.http.MediaType.APPLICATION_PDF_VALUE;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.notifier.TransactionNotifierService;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

    @Mock private UserRestClient userRestClient;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private InvoiceStorageClient invoiceStorageClient;
    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private TransactionNotifierService transactionNotifierService;

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
    void setup() throws IOException {
        srcFile = Files.createTempFile("src-", ".pdf");
        Files.write(srcFile, "content".getBytes());
    }

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(srcFile);
    }

    @Test
    void getPointOfSaleTransactionsWithFiscalCode() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);

        when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE))
                .thenReturn(Mono.just(new FiscalCodeInfoPDV(USER_ID)));

        when(rewardTransactionRepository.findByFilterTrx(any(TrxFiltersDTO.class), eq(POS_ID), eq(USER_ID), eq(""), eq(false), eq(pageable)))
                .thenReturn(Flux.just(trx));

        when(rewardTransactionRepository.getCount(any(TrxFiltersDTO.class), eq(POS_ID), eq(""), eq(USER_ID), eq(false)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POS_ID, "", FISCAL_CODE, STATUS, pageable))
                .assertNext(page -> assertEquals(1, page.getTotalElements()))
                .verifyComplete();
    }

    @Test
    void getPointOfSaleTransactionsWithoutFiscalCode() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(2);

        when(rewardTransactionRepository.findByFilterTrx(any(TrxFiltersDTO.class), eq(POS_ID), isNull(), isNull(), eq(false), eq(pageable)))
                .thenReturn(Flux.just(trx));

        when(rewardTransactionRepository.getCount(any(TrxFiltersDTO.class), eq(POS_ID), isNull(), isNull(), eq(false)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POS_ID, null, null, STATUS, pageable))
                .assertNext(page -> assertEquals(1, page.getTotalElements()))
                .verifyComplete();

        verifyNoInteractions(userRestClient);
    }

    @Test
    void downloadTransactionInvoice_invoiced_ok() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("INVOICED")
                .invoiceData(InvoiceData.builder().filename("invoice.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POS_ID, TRX_ID)).thenReturn(Mono.just(trx));
        when(invoiceStorageClient.getFileSignedUrl(anyString())).thenReturn("tokenUrl");

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .assertNext(dto -> assertEquals("tokenUrl", dto.getInvoiceUrl()))
                .verifyComplete();
    }

    @Test
    void downloadTransactionInvoice_statusNotAllowed_400() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("AUTHORIZED")
                .invoiceData(InvoiceData.builder().filename("x.pdf").build())
                .build();

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POS_ID, TRX_ID)).thenReturn(Mono.just(trx));

        StepVerifier.create(service.downloadTransactionInvoice(MERCHANT_ID, POS_ID, TRX_ID))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verifyNoInteractions(invoiceStorageClient);
    }

    @Test
    void addCreditNoteFile_success(){
        FilePart fp = filePartBackedBySrc("cn.pdf");

        when(invoiceStorageClient.upload(any(), anyString(), any())).thenReturn(null);

        StepVerifier.create(service.addCreditNoteFile(fp, MERCHANT_ID, POS_ID, TRX_ID))
                .verifyComplete();

        verify(invoiceStorageClient, times(1)).upload(any(),
                eq("invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/creditNote/cn.pdf"),
                eq(APPLICATION_PDF_VALUE));
    }


    @Test
    void reversalTransaction_success_decrementsOldBatch(){
        FilePart fp = filePartBackedBySrc("credit-note.pdf");

        RewardTransaction trx = trx();
        trx.setId(TRX_ID);
        trx.setMerchantId(MERCHANT_ID);
        trx.setPointOfSaleId(POS_ID);
        trx.setUserId(USER_ID);
        trx.setStatus(SyncTrxStatus.INVOICED.toString());
        trx.setRewardBatchId("B1");

        RewardBatch batch = new RewardBatch();
        batch.setId("B1");
        batch.setStatus(RewardBatchStatus.CREATED);

        when(rewardTransactionRepository.findTransaction(MERCHANT_ID, POS_ID, TRX_ID)).thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findRewardBatchById("B1")).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.decrementTotals("B1", 123L)).thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        // upload ok
        when(invoiceStorageClient.upload(any(), anyString(), any())).thenReturn(null);

        when(transactionNotifierService.notify(any(), any())).thenReturn(true);

        StepVerifier.create(service.reversalTransaction(TRX_ID, MERCHANT_ID, POS_ID, fp, DOC_NUMBER))
                .verifyComplete();

        verify(rewardBatchRepository, times(1)).decrementTotals("B1", 123L);
        verify(rewardTransactionRepository, times(1)).save(any());
    }

    /* ---------------- helpers ---------------- */

    private RewardTransaction trx() {
        Reward r = new Reward();
        r.setAccruedRewardCents(123L);

        RewardTransaction trx = new RewardTransaction();
        trx.setUpdateDate(LocalDateTime.now().minusDays(1));
        trx.setInitiatives(List.of(INITIATIVE_ID));
        trx.setRewards(Map.of(INITIATIVE_ID, r));
        return trx;
    }

    private FilePart mockFilePart(String filename) {
        FilePart filePart = mock(FilePart.class);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType(APPLICATION_PDF_VALUE));
        when(filePart.filename()).thenReturn(filename);
        when(filePart.headers()).thenReturn(headers);
        return filePart;
    }

    private FilePart filePartBackedBySrc(String filename){
        FilePart fp = mockFilePart(filename);
        when(fp.transferTo(any(Path.class))).thenAnswer(inv -> {
            Path target = inv.getArgument(0);
            Files.copy(srcFile, target, StandardCopyOption.REPLACE_EXISTING);
            return Mono.empty();
        });
        return fp;
    }
}
