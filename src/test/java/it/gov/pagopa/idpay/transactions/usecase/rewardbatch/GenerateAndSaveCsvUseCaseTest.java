package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.Map;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenerateAndSaveCsvUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private UserRestClient userRestClient;
    @Mock private ApprovedRewardBatchBlobService approvedRewardBatchBlobService;
    private GenerateAndSaveCsvUseCase useCase;
    private GenerateAndSaveCsvUseCase useCaseSpy;
    private static final String BATCH_ID = "BATCH_ID";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String MERCHANT_ID = "MERCHANT_ID";

    @BeforeEach
    void setup() {
        useCase = new GenerateAndSaveCsvUseCase(rewardBatchRepository, rewardTransactionRepository, userRestClient, approvedRewardBatchBlobService);
        useCaseSpy = spy(useCase);
    }

    @Test
    void execute_invalidBatchId_fastFail() {
        StepVerifier.create(useCase.execute("bad/..", INITIATIVE_ID, MERCHANT_ID)).expectError(IllegalArgumentException.class).verify();
        verify(rewardBatchRepository, never()).findById(anyString());
    }

    @Test
    void execute_success_withCFandWithoutCF() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).businessName("Biz").name("dicembre 2025").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        RewardTransaction trxWithCF = RewardTransaction.builder().id("T1").trxChargeDate(LocalDateTime.of(2025, 12, 10, 10, 30)).fiscalCode("CF1").trxCode("CODE").effectiveAmountCents(1000L).rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).additionalProperties(Map.of("productName", "Lavatrice", "productGtin", "803")).invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().docNumber("DOC").filename("inv.pdf").build()).franchiseName("Store").build();
        RewardTransaction trxNoCF = RewardTransaction.builder().id("T2").userId("U2").fiscalCode(null).trxCode("CODE2").effectiveAmountCents(2000L).rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build())).additionalProperties(Map.of("productName", "Aspirapolvere", "productGtin", "123")).invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().docNumber(null).filename("inv2.pdf").build()).franchiseName("Store2").build();
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList())).thenReturn(Flux.just(trxWithCF, trxNoCF));
        when(userRestClient.retrieveUserInfo("U2")).thenReturn(Mono.just(it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV.builder().pii("CF2").build()));
        doReturn(Mono.just("some/path/file.csv")).when(useCaseSpy).uploadCsvToBlob(anyString(), anyString());
        StepVerifier.create(useCaseSpy.execute(BATCH_ID, INITIATIVE_ID, MERCHANT_ID)).assertNext(filename -> assertTrue(filename.endsWith(".csv"))).verifyComplete();
        verify(rewardBatchRepository).save(argThat(b -> b.getFilename() != null && b.getFilename().endsWith(".csv")));
        assertEquals("CF2", trxNoCF.getFiscalCode());
    }

    @Test
    void execute_uploadFails_propagates() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).businessName("Biz").name("dicembre 2025").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        RewardTransaction trx = RewardTransaction.builder().id("T1").fiscalCode("CF").trxCode("CODE").effectiveAmountCents(1000L).rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).additionalProperties(Map.of()).invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().filename("inv.pdf").build()).franchiseName("Store").build();
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList())).thenReturn(Flux.just(trx));
        doReturn(Mono.error(new RuntimeException("upload fail"))).when(useCaseSpy).uploadCsvToBlob(anyString(), anyString());
        StepVerifier.create(useCaseSpy.execute(BATCH_ID, INITIATIVE_ID, MERCHANT_ID)).expectError(RuntimeException.class).verify();
        verify(rewardBatchRepository, never()).save(any());
    }

    @Test
    void csvField_behavior() {
        assertEquals("", useCase.csvField(null));
        assertEquals("plain", useCase.csvField("plain"));
        assertEquals("\"a;b\"", useCase.csvField("a;b"));
        assertEquals("\"a\"\"b\"", useCase.csvField("a\"b"));
    }

    @Test
    void mapTransactionToCsvRow_handlesNullsAndFormatting() {
        RewardTransaction trx = RewardTransaction.builder().id("T1").trxChargeDate(null).fiscalCode("CF").trxCode("CODE").effectiveAmountCents(null).rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(null).build())).additionalProperties(Map.of("productName", "Prod;X", "productGtin", "GTIN")).invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().docNumber(null).filename("inv.pdf").build()).franchiseName("Store").build();
        String row = useCase.mapTransactionToCsvRow(trx, INITIATIVE_ID);
        assertNotNull(row);
        assertTrue(row.contains("T1"));
        assertTrue(row.contains("\"Prod;X"));
    }

    @Test
    void uploadCsvToBlob_success_status201() {
        @SuppressWarnings("unchecked") Response<BlockBlobItem> resp = Mockito.mock(Response.class);
        when(resp.getStatusCode()).thenReturn(HttpStatus.CREATED.value());
        when(approvedRewardBatchBlobService.upload(any(), anyString(), anyString())).thenReturn(resp);
        StepVerifier.create(useCase.uploadCsvToBlob("file.csv", "content")).expectNext("file.csv").verifyComplete();
    }

    @Test
    void uploadCsvToBlob_statusNot201_throwsClientExceptionWithBody() {
        @SuppressWarnings("unchecked") Response<BlockBlobItem> resp = Mockito.mock(Response.class);
        when(resp.getStatusCode()).thenReturn(HttpStatus.BAD_REQUEST.value());
        when(approvedRewardBatchBlobService.upload(any(), anyString(), anyString())).thenReturn(resp);
        StepVerifier.create(useCase.uploadCsvToBlob("file.csv", "content")).expectErrorSatisfies(ex -> {
            assertInstanceOf(ClientExceptionWithBody.class, ex);
            assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, ((ClientExceptionWithBody) ex).getCode());
        }).verify();
    }
}

