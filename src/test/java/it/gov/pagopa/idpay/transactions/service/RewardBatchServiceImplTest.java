package it.gov.pagopa.idpay.transactions.service;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.MERCHANT_OR_OPERATOR_HEADER_MANDATORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;
import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadRewardBatchResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;

import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import java.time.LocalDate;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.*;

import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

  @Mock
  private RewardBatchRepository rewardBatchRepository;
  @Mock
  private RewardTransactionRepository rewardTransactionRepository;
  @Mock
  private UserRestClient userRestClient;

  private RewardBatchService rewardBatchService;
  private RewardBatchServiceImpl rewardBatchServiceSpy;

  @Mock
  private ApprovedRewardBatchBlobService approvedRewardBatchBlobService;

  private static final String BUSINESS_NAME = "Test Business name";
  private static final String REWARD_BATCH_ID_1 = "REWARD_BATCH_ID_1";
  private static final String REWARD_BATCH_ID_2 = "REWARD_BATCH_ID_2";
  private static final String INITIATIVE_ID = "INITIATIVE_ID";
  private static final String MERCHANT_ID = "MERCHANT_ID";
  private static final  RewardBatch REWARD_BATCH_1 = RewardBatch.builder()
            .id(REWARD_BATCH_ID_1)
            .build();
    private static final  RewardBatch REWARD_BATCH_2 = RewardBatch.builder()
            .id(REWARD_BATCH_ID_2)
            .build();

    private static final List<String> BATCH_IDS = Arrays.asList(REWARD_BATCH_ID_1, REWARD_BATCH_ID_2);
    private static final String CURRENT_MONTH = "2025-12";
    private static final String NEXT_MONTH = "2026-01";
    private static final String CURRENT_MONTH_NAME = "dicembre 2025";
    private static final String NEXT_MONTH_NAME = "gennaio 2026";



    private static final String FAKE_CSV_FILENAME = "test/path/report_fake.csv";
    private String capturedFilename;

    private static final String VALID_ROLE = "L1";
    private static final String INVALID_ROLE = "GUEST";


    @BeforeEach
  void setUp() {
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionRepository, userRestClient, approvedRewardBatchBlobService);
    rewardBatchServiceSpy = spy((RewardBatchServiceImpl) rewardBatchService);
  }

    @Test
    void handleSuspendedTransactions_whenNoSuspended_shouldReturnOriginal() {
        RewardBatch originalBatch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .numberOfTransactionsSuspended(0L)
                .build();
        Mono<RewardBatch> result = ReflectionTestUtils.invokeMethod(
                rewardBatchServiceSpy, "handleSuspendedTransactions", originalBatch, INITIATIVE_ID);

        StepVerifier.create(result)
                .expectNext(originalBatch)
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(argThat(b -> !b.getId().equals(REWARD_BATCH_ID_1)));
    }

    @Test
    void handleSuspendedTransactions_whenSuspendedExist_shouldCreateNewBatchAndMoveTrx() {
        RewardBatch originalBatch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .numberOfTransactionsSuspended(5L)
                .merchantId(MERCHANT_ID)
                .build();

        RewardBatch newBatch = RewardBatch.builder()
                .id("NEW_BATCH_ID")
                .build();

        doReturn(Mono.just(newBatch)).when(rewardBatchServiceSpy).createRewardBatchAndSave(any());
        doReturn(Mono.just(1000L)).when(rewardBatchServiceSpy)
                .updateAndSaveRewardTransactionsSuspended(eq(REWARD_BATCH_ID_1), eq(INITIATIVE_ID), eq("NEW_BATCH_ID"));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenReturn(Mono.just(newBatch));

        Mono<RewardBatch> result = ReflectionTestUtils.invokeMethod(
                rewardBatchServiceSpy, "handleSuspendedTransactions", originalBatch, INITIATIVE_ID);

        StepVerifier.create(result)
                .expectNext(originalBatch)
                .verifyComplete();

        verify(rewardBatchServiceSpy).updateNewBatchCounters(eq(newBatch), eq(1000L), eq(5L));
        verify(rewardBatchRepository).save(newBatch);
    }

    @Test
    void updateNewBatchCounters_shouldHandleNullFields() {
        RewardBatch newBatch = RewardBatch.builder()
                .id("NEW_BATCH_ID")
                .build();

        Long totalAccrued = 5000L;
        long countToMove = 10L;

        rewardBatchServiceSpy.updateNewBatchCounters(newBatch, totalAccrued, countToMove);

        Assertions.assertEquals(5000L, newBatch.getInitialAmountCents());
        Assertions.assertEquals(10L, newBatch.getNumberOfTransactionsSuspended());
        Assertions.assertEquals(10L, newBatch.getNumberOfTransactions());
    }

    @Test
    void updateNewBatchCounters_shouldAccumulateValues() {
        RewardBatch existingBatch = RewardBatch.builder()
                .id("EXISTING_BATCH_ID")
                .initialAmountCents(1000L)
                .numberOfTransactionsSuspended(5L)
                .numberOfTransactions(20L)
                .build();

        Long totalAccrued = 2500L;
        long countToMove = 3L;

        rewardBatchServiceSpy.updateNewBatchCounters(existingBatch, totalAccrued, countToMove);

        Assertions.assertEquals(3500L, existingBatch.getInitialAmountCents());
        Assertions.assertEquals(8L, existingBatch.getNumberOfTransactionsSuspended());
        Assertions.assertEquals(23L, existingBatch.getNumberOfTransactions());
    }
    private RewardTransaction createMockTransaction(String id, Long effectiveAmount, Long accruedReward, String gtin, String name) {
        RewardTransaction trx = new RewardTransaction();
        trx.setId(id);
        trx.setTrxChargeDate(LocalDateTime.of(2025, 12, 10, 10, 30, 0));
        trx.setFiscalCode("RSSMRA70A01H501U");
        trx.setTrxCode("TXCODE123");
        trx.setEffectiveAmountCents(effectiveAmount);
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);

        Map<String, Reward> rewards = new HashMap<>();
        Reward reward = new Reward();
        reward.setAccruedRewardCents(accruedReward);
        rewards.put(INITIATIVE_ID, reward);
        trx.setRewards(rewards);

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("productGtin", gtin);
        additionalProperties.put("productName", name);
        trx.setAdditionalProperties(additionalProperties);

        InvoiceData invoice = new InvoiceData();
        invoice.setDocNumber("DOC001");
        invoice.setFilename("invoice.pdf");
        trx.setInvoiceData(invoice);

        return trx;
    }

    @Test
    void testGenerateAndSaveCsv_Success() {

        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .merchantId(MERCHANT_ID)
                .businessName("Business")
                .name("BatchName")
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        RewardTransaction trx1 = createMockTransaction(
                "T001", 10000L, 500L, "8033675155005", "Lavatrice");
        RewardTransaction trx2 = createMockTransaction(
                "T002", 500L, 10L, "1234567890123", "Aspirapolvere");
        trx2.setFiscalCode(null);

        when(rewardTransactionRepository.findByFilter(
                REWARD_BATCH_ID_1,
                INITIATIVE_ID,
                List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED)
        )).thenReturn(Flux.just(trx1, trx2));

        when(userRestClient.retrieveUserInfo(trx2.getUserId()))
                .thenReturn(Mono.just(UserInfoPDV.builder().pii("CF_2").build()));

        @SuppressWarnings("unchecked")
        Response<BlockBlobItem> mockResponseSuccess = Mockito.mock(Response.class);
        when(mockResponseSuccess.getStatusCode()).thenReturn(HttpStatus.CREATED.value());

        doAnswer(invocation -> {
            capturedFilename = invocation.getArgument(1);
            return mockResponseSuccess;
        }).when(approvedRewardBatchBlobService).upload(
                any(InputStream.class),
                any(String.class),
                any(String.class)
        );

        String expectedReportFilename = "Business_BatchName_PHYSICAL.csv";

        StepVerifier.create(
                        rewardBatchServiceSpy.generateAndSaveCsv(
                                REWARD_BATCH_ID_1, INITIATIVE_ID, MERCHANT_ID))
                .expectNext(expectedReportFilename)
                .verifyComplete();

        assertThat(capturedFilename)
                .endsWith(expectedReportFilename)
                .contains(REWARD_BATCH_ID_1)
                .contains(MERCHANT_ID);
    }

    @Test
    void testGenerateAndSaveCsv_RepositoryReturnsNoTransactions() {

        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_2)
                .merchantId(MERCHANT_ID)
                .businessName("Business")
                .name("BatchName")
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_2))
                .thenReturn(Mono.just(batch));

        when(rewardTransactionRepository.findByFilter(
                any(), any(), anyList()))
                .thenReturn(Flux.empty());

        when(rewardBatchServiceSpy.uploadCsvToBlob(anyString(), anyString()))
                .thenReturn(Mono.just("Business_BatchName_PHYSICAL.csv"));

        StepVerifier.create(
                        rewardBatchServiceSpy.generateAndSaveCsv(
                                REWARD_BATCH_ID_2, INITIATIVE_ID, MERCHANT_ID))
                .expectNext("Business_BatchName_PHYSICAL.csv")
                .verifyComplete();

        verify(rewardTransactionRepository).findByFilter(any(), any(), anyList());
        verify(rewardBatchServiceSpy).uploadCsvToBlob(anyString(), anyString());
    }

    @Test
    void testGenerateAndSaveCsv_SavingFails() {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .merchantId(MERCHANT_ID)
                .businessName("Business")
                .name("BatchName")
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        RewardTransaction trx = createMockTransaction("T003", 100L, 10L, "1", "Prod");
        when(rewardTransactionRepository.findByFilter(any(), any(), anyList()))
                .thenReturn(Flux.just(trx));

        doReturn(Mono.error(new RuntimeException("Simulated I/O Error")))
                .when(rewardBatchServiceSpy).uploadCsvToBlob(anyString(), anyString());

        StepVerifier.create(
                        rewardBatchServiceSpy.generateAndSaveCsv(REWARD_BATCH_ID_1, INITIATIVE_ID, MERCHANT_ID))
                .expectErrorSatisfies(throwable ->
                        assertThat(throwable)
                                .isInstanceOf(RuntimeException.class)
                                .hasMessageContaining("Simulated I/O Error")
                )
                .verify();

        verify(rewardBatchRepository).findById(REWARD_BATCH_ID_1);
        verify(rewardTransactionRepository).findByFilter(any(), any(), anyList());
        verify(rewardBatchServiceSpy).uploadCsvToBlob(anyString(), anyString());
    }

  @Test
  void testGenerateAndSaveCsv_InvalidBatchId_ThrowsError() {
    String invalidBatchId1 = "batch_id/../secret";

    StepVerifier.create(rewardBatchServiceSpy.generateAndSaveCsv(invalidBatchId1, INITIATIVE_ID, MERCHANT_ID))
        .expectErrorSatisfies(throwable ->
            assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid batch id for CSV file generation")
        )
        .verify();

    String invalidBatchId2 = "batch_id/other";

    StepVerifier.create(rewardBatchServiceSpy.generateAndSaveCsv(invalidBatchId2, INITIATIVE_ID, MERCHANT_ID))
        .expectErrorSatisfies(throwable ->
            assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid batch id for CSV file generation")
        )
        .verify();

    String invalidBatchId3 = "batch_id\\other";

    StepVerifier.create(rewardBatchServiceSpy.generateAndSaveCsv(invalidBatchId3, INITIATIVE_ID, MERCHANT_ID))
        .expectErrorSatisfies(throwable ->
            assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid batch id for CSV file generation")
        )
        .verify();

    verify(rewardTransactionRepository, never()).findByFilter(any(), any(), anyList());
    verify(rewardBatchServiceSpy, never()).uploadCsvToBlob(anyString(), anyString());
  }

  @Test
  void createRewardBatchAndSave_Success_NewBatchCreated() {

        REWARD_BATCH_1.setMonth(CURRENT_MONTH);
        REWARD_BATCH_1.setName(CURRENT_MONTH_NAME);
        REWARD_BATCH_1.setMerchantId("MERCHANT_ID");
        REWARD_BATCH_1.setPosType(PHYSICAL);
        REWARD_BATCH_1.setNumberOfTransactionsSuspended(1L);

    doReturn(NEXT_MONTH).when((RewardBatchServiceImpl) rewardBatchServiceSpy)
        .addOneMonth(CURRENT_MONTH);
    doReturn(NEXT_MONTH_NAME).when((RewardBatchServiceImpl) rewardBatchServiceSpy)
        .addOneMonthToItalian(CURRENT_MONTH_NAME);

        when(rewardBatchRepository.findRewardBatchByFilter(
                null,
                "MERCHANT_ID",
                PHYSICAL,
                NEXT_MONTH))
                .thenReturn(Mono.empty());

    when(rewardBatchRepository.save(any(RewardBatch.class)))
        .thenAnswer(invocation -> {
          RewardBatch newBatch = invocation.getArgument(0);
          newBatch.setId(REWARD_BATCH_ID_2);
          return Mono.just(newBatch);
        });

    Mono<RewardBatch> resultMono = rewardBatchServiceSpy.createRewardBatchAndSave(REWARD_BATCH_1);

    StepVerifier.create(resultMono)
        .assertNext(result -> {
          assertEquals(REWARD_BATCH_ID_2, result.getId());
          assertEquals(NEXT_MONTH, result.getMonth());
          assertEquals(RewardBatchStatus.CREATED, result.getStatus());
          assertEquals(0L, result.getApprovedAmountCents());
          assertEquals(0L, result.getNumberOfTransactionsSuspended());
          assertEquals(RewardBatchAssignee.L1, result.getAssigneeLevel());

        })
        .verifyComplete();

    verify(rewardBatchRepository, times(1)).save(any(RewardBatch.class));
    verify(rewardBatchRepository, times(1)).findRewardBatchByFilter(any(), any(), any(), any());
  }


    @Test
    void createRewardBatchAndSave_Success_ExistingBatchFound() {
      REWARD_BATCH_1.setMonth(CURRENT_MONTH);
      REWARD_BATCH_1.setName(CURRENT_MONTH_NAME);
      REWARD_BATCH_1.setMerchantId("MERCHANT_ID");
      REWARD_BATCH_1.setPosType(PHYSICAL);
      REWARD_BATCH_1.setNumberOfTransactionsSuspended(1L);

      REWARD_BATCH_2.setMonth(NEXT_MONTH);
      REWARD_BATCH_2.setName(NEXT_MONTH_NAME);
      REWARD_BATCH_2.setMerchantId("MERCHANT_ID");
      REWARD_BATCH_2.setPosType(PHYSICAL);


        doReturn(NEXT_MONTH).when( rewardBatchServiceSpy).addOneMonth(CURRENT_MONTH);
        doReturn(NEXT_MONTH_NAME).when( rewardBatchServiceSpy).addOneMonthToItalian(CURRENT_MONTH_NAME);

      when(rewardBatchRepository.findRewardBatchByFilter(
          null,
          "MERCHANT_ID",
          PHYSICAL,
          NEXT_MONTH))
          .thenReturn(Mono.just(REWARD_BATCH_2));

    Mono<RewardBatch> resultMono = rewardBatchServiceSpy.createRewardBatchAndSave(REWARD_BATCH_1);

    StepVerifier.create(resultMono)
        .assertNext(result -> {
          assertEquals(REWARD_BATCH_2.getId(), result.getId());
          assertEquals(NEXT_MONTH, result.getMonth());

        })
        .verifyComplete();

    verify(rewardBatchRepository, never()).save(any(RewardBatch.class));
    verify(rewardBatchRepository, times(1)).findRewardBatchByFilter(any(), any(), any(), any());
  }

  @Test
  void processSingleBatch_NotFound() {
    when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(Mono.empty());

    StepVerifier.create(rewardBatchServiceSpy.processSingleBatch(REWARD_BATCH_ID_1, INITIATIVE_ID))
        .verifyErrorMatches(e -> e instanceof ClientExceptionWithBody
            && e.getMessage().equalsIgnoreCase(
            ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(
                REWARD_BATCH_ID_1)));

    verify(rewardBatchRepository, never()).save(any());
  }

  @Test
  void processSingleBatch_InvalidState() {
    REWARD_BATCH_1.setStatus(RewardBatchStatus.EVALUATING);
    when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(
        Mono.just(REWARD_BATCH_1));

    StepVerifier.create(rewardBatchServiceSpy.processSingleBatch(REWARD_BATCH_ID_1, INITIATIVE_ID))
        .verifyErrorMatches(e -> e instanceof ClientExceptionWithBody
            && e.getMessage().equalsIgnoreCase(
            ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(
                REWARD_BATCH_ID_1)));

    verify(rewardBatchRepository, never()).save(any());
  }

  @Test
  void processSingleBatch_Success_NoSuspended() {
    REWARD_BATCH_1.setStatus(RewardBatchStatus.APPROVING);
    REWARD_BATCH_1.setAssigneeLevel(RewardBatchAssignee.L3);
    REWARD_BATCH_1.setNumberOfTransactionsSuspended(0L);

    doReturn(Mono.just(FAKE_CSV_FILENAME))
        .when(rewardBatchServiceSpy)
        .generateAndSaveCsv(anyString(), anyString(), anyString());

    when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(REWARD_BATCH_1));
    doReturn(Mono.empty()).when(rewardBatchServiceSpy).updateAndSaveRewardTransactionsToApprove(anyString(), anyString());
    when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(rewardBatchServiceSpy.processSingleBatch(REWARD_BATCH_ID_1, INITIATIVE_ID))
        .expectNextMatches(batch ->
            batch.getId().equals(REWARD_BATCH_ID_1) &&
                batch.getStatus().equals(RewardBatchStatus.APPROVED) &&
                batch.getNumberOfTransactionsSuspended().equals(0L) &&
                batch.getUpdateDate() != null)
        .verifyComplete();

            verify(rewardBatchServiceSpy, times(1)).updateAndSaveRewardTransactionsToApprove(REWARD_BATCH_ID_1, INITIATIVE_ID);
            verify(rewardBatchServiceSpy, never()).createRewardBatchAndSave(any());
            verify(rewardBatchRepository, times(2)).save(any(RewardBatch.class));
        }

  @Test
  void processSingleBatch_Success_WithSuspended() {
    REWARD_BATCH_1.setStatus(RewardBatchStatus.APPROVING);
    REWARD_BATCH_1.setAssigneeLevel(RewardBatchAssignee.L3);
    REWARD_BATCH_1.setNumberOfTransactionsSuspended(1L);
    REWARD_BATCH_2.setStatus(RewardBatchStatus.CREATED);

            doReturn(Mono.just(FAKE_CSV_FILENAME))
                    .when(rewardBatchServiceSpy)
                    .generateAndSaveCsv(anyString(), anyString(), anyString());

            when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(REWARD_BATCH_1));
            doReturn(Mono.empty()).when(rewardBatchServiceSpy).updateAndSaveRewardTransactionsToApprove(anyString(), anyString());
            doReturn(Mono.just(REWARD_BATCH_2)).when(rewardBatchServiceSpy).createRewardBatchAndSave(any(RewardBatch.class));
            doReturn(Mono.empty()).when(rewardBatchServiceSpy).updateAndSaveRewardTransactionsSuspended(anyString(), anyString(), anyString());
            when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));


            StepVerifier.create(rewardBatchServiceSpy.processSingleBatch(REWARD_BATCH_ID_1, INITIATIVE_ID))
                    .expectNextMatches(batch ->
                            batch.getId().equals(REWARD_BATCH_ID_1) &&
                                    batch.getStatus().equals(RewardBatchStatus.APPROVED) &&
                                    batch.getNumberOfTransactionsSuspended().equals(1L) &&
                                    batch.getUpdateDate() != null)
                    .verifyComplete();

            verify(rewardBatchServiceSpy, times(1)).updateAndSaveRewardTransactionsToApprove(REWARD_BATCH_ID_1, INITIATIVE_ID);
            verify(rewardBatchServiceSpy, times(1)).createRewardBatchAndSave(REWARD_BATCH_1);
            verify(rewardBatchServiceSpy, times(1)).updateAndSaveRewardTransactionsSuspended(
                    REWARD_BATCH_ID_1,
                    INITIATIVE_ID,
                    REWARD_BATCH_ID_2
            );
            verify(rewardBatchRepository, times(2)).save(argThat(batch ->
                    batch.getId().equals(REWARD_BATCH_ID_1) && batch.getStatus().equals(RewardBatchStatus.APPROVED)));
        }


  @Test
  void processSingleBatchSafe_Success() {
    doReturn(Mono.just(REWARD_BATCH_1)).when(rewardBatchServiceSpy)
        .processSingleBatch(eq(REWARD_BATCH_ID_1), anyString());

    Mono<RewardBatch> result = rewardBatchServiceSpy.processSingleBatchSafe(REWARD_BATCH_ID_1,
        INITIATIVE_ID);

    StepVerifier.create(result).expectNext(REWARD_BATCH_1).verifyComplete();

  }

  @Test
  void processSingleBatchSafe_HandlesClientExceptionAndCompletesEmpty() {
    ClientExceptionWithBody error = new ClientExceptionWithBody(
        HttpStatus.NOT_FOUND,
        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND,
        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(
            REWARD_BATCH_ID_2)
    );

    doReturn(Mono.error(error)).when(rewardBatchServiceSpy)
        .processSingleBatch(eq(REWARD_BATCH_ID_2), anyString());

    Mono<RewardBatch> result = rewardBatchServiceSpy.processSingleBatchSafe(REWARD_BATCH_ID_2,
        INITIATIVE_ID);

    StepVerifier.create(result).verifyComplete();

  }

  @Test
  void rewardBatchConfirmationBatch_WithList_AllSuccess() {
    doReturn(Mono.just(REWARD_BATCH_1)).when(rewardBatchServiceSpy)
        .processSingleBatchSafe(eq(REWARD_BATCH_ID_1), anyString());
    doReturn(Mono.just(REWARD_BATCH_2)).when(rewardBatchServiceSpy)
        .processSingleBatchSafe(eq(REWARD_BATCH_ID_2), anyString());

    Mono<Void> result = rewardBatchServiceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID,
        BATCH_IDS);

    StepVerifier.create(result).verifyComplete();

    verify(rewardBatchServiceSpy, times(1)).processSingleBatchSafe(REWARD_BATCH_ID_1,
        INITIATIVE_ID);
    verify(rewardBatchServiceSpy, times(1)).processSingleBatchSafe(REWARD_BATCH_ID_2,
        INITIATIVE_ID);
    verify(rewardBatchRepository, never()).findRewardBatchByStatus(any());
  }

  @Test
  void rewardBatchConfirmationBatch_WithList_PartialSuccess_FailureIgnored() {
    doReturn(Mono.empty()).when(rewardBatchServiceSpy)
        .processSingleBatchSafe(eq(REWARD_BATCH_ID_1), anyString());
    doReturn(Mono.just(REWARD_BATCH_2)).when(rewardBatchServiceSpy)
        .processSingleBatchSafe(eq(REWARD_BATCH_ID_2), anyString());

    Mono<Void> result = rewardBatchServiceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID,
        BATCH_IDS);

    StepVerifier.create(result).verifyComplete();

    verify(rewardBatchServiceSpy, times(1)).processSingleBatchSafe(REWARD_BATCH_ID_1,
        INITIATIVE_ID);
    verify(rewardBatchServiceSpy, times(1)).processSingleBatchSafe(REWARD_BATCH_ID_2,
        INITIATIVE_ID);
  }


  @Test
  void rewardBatchConfirmationBatch_NoList_MultipleBatchesFound() {
    List<RewardBatch> foundBatches = Arrays.asList(REWARD_BATCH_1, REWARD_BATCH_2);

    when(rewardBatchRepository.findRewardBatchByStatus(RewardBatchStatus.APPROVING))
        .thenReturn(Flux.fromIterable(foundBatches));

    doReturn(Mono.just(REWARD_BATCH_1)).when(rewardBatchServiceSpy)
        .processSingleBatchSafe(eq(REWARD_BATCH_ID_1), anyString());
    doReturn(Mono.just(REWARD_BATCH_2)).when(rewardBatchServiceSpy)
        .processSingleBatchSafe(eq(REWARD_BATCH_ID_2), anyString());

    Mono<Void> result = rewardBatchServiceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID,
        Collections.emptyList());

    StepVerifier.create(result).verifyComplete();

    verify(rewardBatchServiceSpy, times(1)).processSingleBatchSafe(REWARD_BATCH_ID_1,
        INITIATIVE_ID);
    verify(rewardBatchServiceSpy, times(1)).processSingleBatchSafe(REWARD_BATCH_ID_2,
        INITIATIVE_ID);
  }

  @Test
  void rewardBatchConfirmationBatch_NoList_ZeroBatchesFound_LogAndComplete() {
    when(rewardBatchRepository.findRewardBatchByStatus(RewardBatchStatus.APPROVING))
        .thenReturn(Flux.empty());

    Mono<Void> result = rewardBatchServiceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID, null);

    StepVerifier.create(result).verifyComplete();
    verify(rewardBatchServiceSpy, never()).processSingleBatchSafe(anyString(), anyString());
  }

  @Test
  void findOrCreateBatch_createsNewBatch() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PHYSICAL, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PHYSICAL, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == PHYSICAL;
          assert batch.getStatus() == RewardBatchStatus.CREATED;
          assert batch.getName().contains("novembre 2025");
          assert batch.getStartDate().equals(yearMonth.atDay(1).atStartOfDay());
          assert batch.getEndDate().equals(yearMonth.atEndOfMonth().atTime(23, 59, 59));
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).save(any());
  }

  @Test
  void findOrCreateBatch_existingBatch() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    RewardBatch existingBatch = RewardBatch.builder()
        .id("BATCH1")
        .merchantId("M1")
        .posType(PHYSICAL)
        .month(batchMonth)
        .status(RewardBatchStatus.CREATED)
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL,
            batchMonth))
        .thenReturn(Mono.just(existingBatch));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PHYSICAL, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == PHYSICAL;
          assert batch.getStatus() == RewardBatchStatus.CREATED;
          assert batch.getName().contains("novembre 2025");
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository, Mockito.never()).save(any());
  }

  @Test
  void findOrCreateBatch_handlesDuplicateKeyException() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();
    PosType posType = PHYSICAL;

    RewardBatch existingBatch = RewardBatch.builder()
        .id("BATCH_DUP")
        .merchantId("M1")
        .posType(posType)
        .month(batchMonth)
        .status(RewardBatchStatus.CREATED)
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", posType, batchMonth))
        .thenReturn(Mono.empty())
        .thenReturn(Mono.just(existingBatch));

    Mockito.when(rewardBatchRepository.save(any()))
        .thenReturn(Mono.error(new DuplicateKeyException("Duplicate")));

    StepVerifier.create(
            new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionRepository,userRestClient, approvedRewardBatchBlobService)
                .findOrCreateBatch("M1", posType, batchMonth, BUSINESS_NAME)
        )
        .assertNext(batch -> {
          assert batch.getId().equals("BATCH_DUP");
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == posType;
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository, Mockito.times(2))
        .findByMerchantIdAndPosTypeAndMonth("M1", posType, batchMonth);
    Mockito.verify(rewardBatchRepository).save(any());
  }

  @Test
  void buildBatchName_physicalPos() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PHYSICAL, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PHYSICAL, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().contains("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void buildBatchName_onlinePos() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PosType.ONLINE, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().contains("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void buildBatchName_baseName() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PosType.ONLINE, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().equals("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void getRewardBatches_forMerchant_returnsPagedResult() {
    String merchantId = "M1";
    String organizationRole = null;
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch rb1 = RewardBatch.builder().id("B1").merchantId(merchantId).name("novembre 2025")
        .build();
    RewardBatch rb2 = RewardBatch.builder().id("B2").merchantId(merchantId).name("novembre 2025")
        .build();

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, false,
            pageable)
    ).thenReturn(Flux.just(rb1, rb2));

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, false)
    ).thenReturn(Mono.just(5L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel,
                pageable)
        )
        .assertNext(page -> {
          assertEquals(2, page.getContent().size());
          assertEquals("B1", page.getContent().get(0).getId());
          assertEquals("B2", page.getContent().get(1).getId());
          assertEquals(5, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();
  }

  @Test
  void getRewardBatches_forMerchant_emptyPage() {
    String merchantId = "M1";
    String organizationRole = null;
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(1, 2);

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, false,
            pageable)
    ).thenReturn(Flux.empty());

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, false)
    ).thenReturn(Mono.just(0L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel,
                pageable)
        )
        .assertNext(page -> {
          assertTrue(page.getContent().isEmpty());
          assertEquals(0, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();
  }

  @Test
  void getRewardBatches_forOperator_returnsPagedResult() {
    String merchantId = null;
    String organizationRole = "operator1";
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch batchA = RewardBatch.builder().id("B1").merchantId("MERCHANT1")
        .name("novembre 2025").build();
    RewardBatch batchB = RewardBatch.builder().id("B2").merchantId("MERCHANT2")
        .name("novembre 2025").build();

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, true,
            pageable)
    ).thenReturn(Flux.just(batchA, batchB));

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, true)
    ).thenReturn(Mono.just(10L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel,
                pageable)
        )
        .assertNext(page -> {
          assertEquals(2, page.getContent().size());
          assertEquals(10, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository)
        .findRewardBatchesCombined(merchantId, status, assigneeLevel, true, pageable);
    Mockito.verify(rewardBatchRepository).getCountCombined(merchantId, status, assigneeLevel, true);
  }

  @Test
  void getRewardBatches_forOperator_emptyPage() {
    String merchantId = null;
    String organizationRole = "operator1";
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, true,
            pageable)
    ).thenReturn(Flux.empty());

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, true)
    ).thenReturn(Mono.just(0L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel,
                pageable)
        )
        .assertNext(page -> {
          assertTrue(page.getContent().isEmpty());
          assertEquals(0, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();
  }

  @Test
  void incrementTotals_callsRepository() {
    RewardBatch updated = RewardBatch.builder()
        .id("B1")
        .initialAmountCents(500L)
        .build();

    Mockito.when(rewardBatchRepository.incrementTotals("B1", 200L))
        .thenReturn(Mono.just(updated));

    StepVerifier.create(rewardBatchService.incrementTotals("B1", 200L))
        .expectNextMatches(b -> b.getInitialAmountCents() == 500L)
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).incrementTotals("B1", 200L);
  }

  @Test
  void sendRewardBatch_batchNotFound() {
    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.empty());

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_merchantIdMismatch() {
    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("OTHER")
        .month("2025-11")
        .status(RewardBatchStatus.CREATED)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_invalidStatus() {
    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month("2025-11")
        .status(RewardBatchStatus.SENT)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_monthTooEarly() {
    YearMonth futureMonth = YearMonth.now().plusMonths(2);

    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(futureMonth.toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    when(rewardBatchRepository.findById("B1")).thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectErrorSatisfies(ex -> {
          assertInstanceOf(RewardBatchException.class, ex);
          assertTrue(ex.getMessage().contains("REWARD_BATCH_MONTH_TOO_EARLY"));
        })
        .verify();
  }

  @Test
  void sendRewardBatch_success() {
    YearMonth batchMonth = YearMonth.now().minusMonths(2);

    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(batchMonth.toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosType("M1", PosType.PHYSICAL))
        .thenReturn(Flux.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).save(any());
  }

  @Test
  void isOperator_shouldReturnFalse_whenRoleIsNull() throws Exception {
    var method = RewardBatchServiceImpl.class.getDeclaredMethod("isOperator", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(rewardBatchService, (String) null);
    assertFalse(result);
  }

  @Test
  void isOperator_shouldReturnFalse_whenRoleIsNotOperator() throws Exception {
    var method = RewardBatchServiceImpl.class.getDeclaredMethod("isOperator", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(rewardBatchService, "randomRole");
    assertFalse(result);
  }

  @Test
  void suspendTransactions_ok() {
    String batchId = "BATCH1";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("TX1", "TX2"));
    request.setReason("Check");

    RewardTransaction oldTx1 = new RewardTransaction();
    oldTx1.setId("TX1");
    oldTx1.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
    oldTx1.setRewards(Map.of(
        initiativeId,
        Reward.builder().accruedRewardCents(100L).build()
    ));

    RewardTransaction oldTx2 = new RewardTransaction();
    oldTx2.setId("TX2");
    oldTx2.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
    oldTx2.setRewards(Map.of(
        initiativeId,
        Reward.builder().accruedRewardCents(200L).build()
    ));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "TX1",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(oldTx1));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "TX2",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(oldTx2));

    long expectedElaborated = 1L;
    long expectedSuspended = 2L;
    long expectedApprovedAmount = -300L;
    long expectedRejected = 0L;

    when(rewardBatchRepository.updateTotals(batchId, expectedElaborated, expectedApprovedAmount,
        expectedRejected, expectedSuspended))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNextMatches(result -> result.getId().equals(batchId)
            && result.getStatus() == RewardBatchStatus.EVALUATING)
        .verifyComplete();

    verify(rewardBatchRepository).updateTotals(batchId, expectedElaborated, expectedApprovedAmount,
        expectedRejected, expectedSuspended);
  }

  @Test
  void suspendTransactions_noModifiedTransactions() {
    String batchId = "BATCH1";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("TX3", "TX4"));
    request.setReason("Check");

    RewardTransaction oldTx = new RewardTransaction();
    oldTx.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
    oldTx.setRewards(Map.of());

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(rewardTransactionRepository.updateStatusAndReturnOld(eq(batchId), anyString(),
        eq(RewardBatchTrxStatus.SUSPENDED), eq(request.getReason()), eq(batchMonth)))
        .thenReturn(Mono.just(oldTx));

    when(rewardBatchRepository.updateTotals(batchId, 0L, 0L, 0L, 0L))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();

    verify(rewardBatchRepository).updateTotals(batchId, 0L, 0L, 0L, 0L);
  }

  @Test
  void suspendTransactions_suspendedTotalZero_returnsOriginalBatch() {
    String batchId = "batch123";
    String initiativeId = "init123";
        String batchMonth = "dicembre 2025";

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trx1", "trx2"));

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    RewardTransaction oldTx = new RewardTransaction();
    oldTx.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
    oldTx.setRewards(Map.of(initiativeId,
        Reward.builder().accruedRewardCents(null).build()));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(rewardTransactionRepository.updateStatusAndReturnOld(eq(batchId), anyString(),
        eq(RewardBatchTrxStatus.SUSPENDED), eq(request.getReason()), eq(batchMonth)))
        .thenReturn(Mono.just(oldTx));

    when(rewardBatchRepository.updateTotals(batchId, 2L, 0L, 0L, 2L))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();

    verify(rewardBatchRepository).updateTotals(batchId, 2L, 0L, 0L, 2L);
  }

  @Test
  void suspendTransactions_throwsException_whenBatchApproved_serviceLevel() {
    String batchId = "BATCH_APPROVED";
    String initiativeId = "INIT1";

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trx1"));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.empty());

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectErrorMatches(ex ->
            ex instanceof ClientExceptionWithBody &&
                ex.getMessage().replaceAll("\\s+", " ")
                    .contains("Reward batch BATCH_APPROVED not found or not in a valid state")
        )
        .verify();

    verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(),
        any(), any());
    verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(),
        anyLong());
  }

  @Test
  void suspendTransactions_handlesRejectedTransaction() {
    String batchId = "batchRejected";
    String initiativeId = "initRejected";
        String batchMonth = "dicembre 2025";

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trx1"));

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    RewardTransaction rejectedTx = new RewardTransaction();
    rejectedTx.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);
    rejectedTx.setRewards(Map.of(initiativeId,
        Reward.builder().accruedRewardCents(100L).build()));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(rewardTransactionRepository.updateStatusAndReturnOld(eq(batchId), anyString(),
        eq(RewardBatchTrxStatus.SUSPENDED), eq(request.getReason()), eq(batchMonth)))
        .thenReturn(Mono.just(rejectedTx));

    when(rewardBatchRepository.updateTotals(batchId, 0L, 0L, -1L, 1L))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();

    verify(rewardBatchRepository).updateTotals(batchId, 0L, 0L, -1L, 1L);
  }

  @Test
  void suspendTransactions_handlesNullAndMissingRewards() {
    String batchId = "batchNullRewards";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trxNull", "trxMissing", "trxWithReward"));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxNull",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.empty());

    RewardTransaction trxMissing = new RewardTransaction();
    trxMissing.setId("trxMissing");
    trxMissing.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
    trxMissing.setRewards(Map.of("OTHER_INIT", Reward.builder().accruedRewardCents(50L).build()));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxMissing",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxMissing));

    RewardTransaction trxWithReward = new RewardTransaction();
    trxWithReward.setId("trxWithReward");
    trxWithReward.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
    trxWithReward.setRewards(
        Map.of(initiativeId, Reward.builder().accruedRewardCents(100L).build()));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxWithReward",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxWithReward));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(rewardBatchRepository.updateTotals(batchId, 0L, -100L, 0L, 2L))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();

    verify(rewardBatchRepository).updateTotals(batchId, 0L, -100L, 0L, 2L);

    verify(rewardTransactionRepository).updateStatusAndReturnOld(batchId, "trxNull",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth);
    verify(rewardTransactionRepository).updateStatusAndReturnOld(batchId, "trxMissing",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth);
    verify(rewardTransactionRepository).updateStatusAndReturnOld(batchId, "trxWithReward",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth);
  }

  @Test
  void suspendTransactions_trxSuspended() {
    String batchId = "batchAllStatuses";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trxSuspended"));

    RewardTransaction trxSuspended = new RewardTransaction();
    trxSuspended.setId("trxSuspended");
    trxSuspended.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
    trxSuspended.setRewards(Map.of());

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxSuspended));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(
        rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();
  }

  @Test
  void suspendTransactions_trxApproved() {
    String batchId = "batchAllStatuses";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trxApproved"));

    RewardTransaction trxApproved = new RewardTransaction();
    trxApproved.setId("trxApproved");
    trxApproved.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
    trxApproved.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(100L).build()));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxApproved));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(
        rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();
  }

  @Test
  void suspendTransactions_mixedStatuses() {
    String batchId = "batchAllStatuses";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trxToCheck", "trxConsultable", "trxRejected"));

    RewardTransaction trxToCheck = new RewardTransaction();
    trxToCheck.setId("trxToCheck");
    trxToCheck.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
    trxToCheck.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(null).build()));

    RewardTransaction trxConsultable = new RewardTransaction();
    trxConsultable.setId("trxConsultable");
    trxConsultable.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
    trxConsultable.setRewards(
        Map.of(initiativeId, Reward.builder().accruedRewardCents(50L).build()));

    RewardTransaction trxRejected = new RewardTransaction();
    trxRejected.setId("trxRejected");
    trxRejected.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);
    trxRejected.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(20L).build()));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxToCheck));
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxConsultable));
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxRejected));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(
        rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();
  }

  @Test
  void suspendTransactions_trxOldIsNull() {
    String batchId = "batchOldNull";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(List.of("trxNull"));

    RewardTransaction trxNull = RewardTransaction.builder()
        .id(null)
        .rewards(Collections.emptyMap())
        .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
        .build();

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxNull",
        RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
        .thenReturn(Mono.just(trxNull));

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(
        rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();
  }

  @Test
  void suspendTransactions_eachSwitchCase() {
    String batchId = "batchSwitch";
    String initiativeId = "INIT1";
        String batchMonth = "dicembre 2025";

    RewardBatch batch = RewardBatch.builder()
        .id(batchId)
        .status(RewardBatchStatus.EVALUATING)
        .month(batchMonth)
                .build();

    TransactionsRequest request = new TransactionsRequest();
    request.setTransactionIds(
        List.of("trxApproved", "trxToCheck", "trxConsultable", "trxRejected", "trxSuspended"));

    RewardTransaction trxApproved = RewardTransaction.builder()
        .id("trxApproved")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
        .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(100L).build()))
        .build();

    RewardTransaction trxToCheck = RewardTransaction.builder()
        .id("trxToCheck")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
        .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(50L).build()))
        .build();

    RewardTransaction trxConsultable = RewardTransaction.builder()
        .id("trxConsultable")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
        .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(25L).build()))
        .build();

    RewardTransaction trxRejected = RewardTransaction.builder()
        .id("trxRejected")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
        .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(10L).build()))
        .build();

    RewardTransaction trxSuspended = RewardTransaction.builder()
        .id("trxSuspended")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
        .rewards(Collections.emptyMap())
        .build();

    Map<String, RewardTransaction> trxMap = Map.of(
        "trxApproved", trxApproved,
        "trxToCheck", trxToCheck,
        "trxConsultable", trxConsultable,
        "trxRejected", trxRejected,
        "trxSuspended", trxSuspended
    );

    trxMap.forEach((id, trx) ->
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, id,
            RewardBatchTrxStatus.SUSPENDED, request.getReason(), batchMonth))
            .thenReturn(Mono.just(trx))
    );

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(batch));

    when(
        rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
        .expectNext(batch)
        .verifyComplete();
  }


  @Test
  void rewardBatchConfirmation_Ok() {
    REWARD_BATCH_1.setStatus(RewardBatchStatus.EVALUATING);
    REWARD_BATCH_1.setAssigneeLevel(RewardBatchAssignee.L3);
    REWARD_BATCH_2.setStatus(RewardBatchStatus.APPROVED);

    when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(
        Mono.just(REWARD_BATCH_1));
    when(rewardBatchRepository.findRewardBatchByMonthBefore(any(), any(), any())).thenReturn(
        Flux.just(REWARD_BATCH_2));
    when(rewardBatchRepository.save(any(RewardBatch.class))).thenReturn(Mono.just(REWARD_BATCH_1));

    Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID,
        REWARD_BATCH_ID_1);

    StepVerifier.create(result)
        .expectNextMatches(batch ->
            batch.getId().equals(REWARD_BATCH_ID_1) &&
                batch.getStatus().equals(RewardBatchStatus.APPROVING)
        )
        .verifyComplete();

    verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());

  }

  @Test
  void rewardBatchConfirmation_Failure_NotFound() {
    when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(Mono.empty());

    Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID,
        REWARD_BATCH_ID_1);

    StepVerifier.create(result)
        .expectErrorMatches(throwable ->
            throwable instanceof ClientExceptionWithBody &&
                ((ClientExceptionWithBody) throwable).getHttpStatus().equals(HttpStatus.NOT_FOUND)
        )
        .verify();

    verify(rewardBatchRepository, times(0)).save(any());
    verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());
  }

  @Test
  void rewardBatchConfirmation_Failure_InvalidState() {
    REWARD_BATCH_1.setStatus(RewardBatchStatus.EVALUATING);
    REWARD_BATCH_1.setAssigneeLevel(RewardBatchAssignee.L2);
    when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID_1)).thenReturn(
        Mono.just(REWARD_BATCH_1));

    Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID,
        REWARD_BATCH_ID_1);

    StepVerifier.create(result)
        .expectErrorMatches(throwable ->
            throwable instanceof ClientExceptionWithBody &&
                ((ClientExceptionWithBody) throwable).getHttpStatus().equals(HttpStatus.BAD_REQUEST)
        )
        .verify();

    verify(rewardBatchRepository, times(0)).save(any());
    verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());
  }

  @Test
  void approvedTransactions() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
    String batchMonth = "dicembre 2025";

    TransactionsRequest transactionsRequest = TransactionsRequest.builder()
        .transactionIds(
            List.of("trxApproved", "trxToCheck", "trxConsultable", "trxSuspended", "trxRejected"))
        .build();

    RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING)
        .month(batchMonth).build();

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(rewardBatch));

    //Mock for approved
    Reward rewardApproved = Reward.builder().accruedRewardCents(1000L).build();
    Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, rewardApproved);
    RewardTransaction trxApprovedMock = RewardTransaction.builder()
        .id("trxApproved")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
        .rewards(rewardApprovedMap)
        .rewardBatchLastMonthElaborated(batchMonth).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.just(trxApprovedMock));

    //Mock for to_check
    Reward rewardToCheck = Reward.builder().accruedRewardCents(2000L).build();
    Map<String, Reward> rewardToCheckMap = Map.of(initiativeId, rewardToCheck);
    RewardTransaction trxToCheckMock = RewardTransaction.builder()
        .id("trxToCheck")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
        .rewards(rewardToCheckMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.just(trxToCheckMock));

    //Mock for consultable
    Reward rewardConsultable = Reward.builder().accruedRewardCents(2500L).build();
    Map<String, Reward> rewardConsultableMap = Map.of(initiativeId, rewardConsultable);
    RewardTransaction trxConsultableMock = RewardTransaction.builder()
        .id("trxConsultable")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
        .rewards(rewardConsultableMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.just(trxConsultableMock));

    //Mock for suspended
    Reward rewardSuspended = Reward.builder().accruedRewardCents(3000L).build();
    Map<String, Reward> rewardSuspendedMap = Map.of(initiativeId, rewardSuspended);
    RewardTransaction trxSuspendedMock = RewardTransaction.builder()
        .id("trxSuspended")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
        .rewards(rewardSuspendedMap)
        .rewardBatchLastMonthElaborated(batchMonth).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.just(trxSuspendedMock));

    //Mock for rejected
    Reward rewardRejected = Reward.builder().accruedRewardCents(3500L).build();
    Map<String, Reward> rewardRejectedMap = Map.of(initiativeId, rewardRejected);
    RewardTransaction trxRejectedMock = RewardTransaction.builder()
        .id("trxRejected")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
        .rewards(rewardRejectedMap)
        .rewardBatchLastMonthElaborated(batchMonth).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.just(trxRejectedMock));

    RewardBatch expectedResult = new RewardBatch();
    when(rewardBatchRepository.updateTotals(
        batchId,
        2L, //TO_CHECK and CONSULTABLE
        rewardSuspended.getAccruedRewardCents() + rewardRejected.getAccruedRewardCents(),
        -1L,
        -1L))
        .thenReturn(Mono.just(expectedResult));

    RewardBatch result = rewardBatchService.approvedTransactions(batchId, transactionsRequest,
        initiativeId).block();

    assertNotNull(result);
    Assertions.assertEquals(expectedResult, result);
    verify(rewardTransactionRepository, times(5)).updateStatusAndReturnOld(any(), any(), any(),
        any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
  }

  @Test
  void approvedTransactions_NotFoundBatch() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
    TransactionsRequest transactionsRequest = TransactionsRequest.builder()
        .transactionIds(List.of("trxId")).build();

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.empty());

    Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId,
        transactionsRequest, initiativeId);
    Assertions.assertThrows(ClientExceptionWithBody.class, resultMono::block);

    verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(),
        any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(),
        anyLong());

  }

  @Test
  void approvedTransactions_ErrorInUpdateInModifyTrx() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
    String batchMonth = "dicembre 2025";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

    RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING)
        .month(batchMonth).build();
    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(rewardBatch));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

    Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId,
        transactionsRequest, initiativeId);
    Assertions.assertThrows(RuntimeException.class, resultMono::block);

    verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(),
        anyLong());
  }

  @Test
  void approvedTransactions_ErrorInUpdateBatch() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
    String batchMonth = "dicembre 2025";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

    RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING)
        .month(batchMonth).build();
    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(rewardBatch));

    //Mock for approved
    Reward reward = Reward.builder().accruedRewardCents(1000L).build();
    Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
    RewardTransaction trxMock = RewardTransaction.builder()
        .id("trxId")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
        .rewardBatchId(batchId)
        .rewards(rewardApprovedMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",
        RewardBatchTrxStatus.APPROVED, null, batchMonth))
        .thenReturn(Mono.just(trxMock));

    when(rewardBatchRepository.updateTotals(batchId, 1L, 0L, 0, 0))
        .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

    Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId,
        transactionsRequest, initiativeId);
    Assertions.assertThrows(RuntimeException.class, resultMono::block);

    verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
  }

  @Test
  void rejectTransactions() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
        String batchMonth = "dicembre 2025";

    TransactionsRequest transactionsRequest = TransactionsRequest.builder()
        .transactionIds(
            List.of("trxApproved", "trxToCheck", "trxConsultable", "trxSuspended", "trxRejected"))
        .build();

    RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING)
        .month(batchMonth).build();

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(rewardBatch));

    //Mock for approved
    Reward rewardApproved = Reward.builder().accruedRewardCents(1000L).build();
    Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, rewardApproved);
    RewardTransaction trxApprovedMock = RewardTransaction.builder()
        .id("trxApproved")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
        .rewards(rewardApprovedMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.just(trxApprovedMock));

    //Mock for to_check
    Reward rewardToCheck = Reward.builder().accruedRewardCents(2000L).build();
    Map<String, Reward> rewardToCheckMap = Map.of(initiativeId, rewardToCheck);
    RewardTransaction trxToCheckMock = RewardTransaction.builder()
        .id("trxToCheck")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
        .rewards(rewardToCheckMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.just(trxToCheckMock));

    //Mock for consultable
    Reward rewardConsultable = Reward.builder().accruedRewardCents(2500L).build();
    Map<String, Reward> rewardConsultableMap = Map.of(initiativeId, rewardConsultable);
    RewardTransaction trxConsultableMock = RewardTransaction.builder()
        .id("trxConsultable")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
        .rewards(rewardConsultableMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.just(trxConsultableMock));

    //Mock for suspended
    Reward rewardSuspended = Reward.builder().accruedRewardCents(3000L).build();
    Map<String, Reward> rewardSuspendedMap = Map.of(initiativeId, rewardSuspended);
    RewardTransaction trxSuspendedMock = RewardTransaction.builder()
        .id("trxSuspended")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
        .rewards(rewardSuspendedMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.just(trxSuspendedMock));

    //Mock for rejected
    Reward rewardRejected = Reward.builder().accruedRewardCents(3500L).build();
    Map<String, Reward> rewardRejectedMap = Map.of(initiativeId, rewardRejected);
    RewardTransaction trxRejectedMock = RewardTransaction.builder()
        .id("trxRejected")
        .rewardBatchId(batchId)
        .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
        .rewards(rewardRejectedMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.just(trxRejectedMock));

    RewardBatch expectedResult = new RewardBatch();
    when(rewardBatchRepository.updateTotals(
        batchId,
        2L, //TO_CHECK and CONSULTABLE
        -rewardApproved.getAccruedRewardCents() - rewardToCheck.getAccruedRewardCents()
            - rewardConsultable.getAccruedRewardCents(),
        4L,
        -1L))
        .thenReturn(Mono.just(expectedResult));

    RewardBatch result = rewardBatchService.rejectTransactions(batchId, initiativeId,
        transactionsRequest).block();

    assertNotNull(result);
    Assertions.assertEquals(expectedResult, result);
    verify(rewardTransactionRepository, times(5)).updateStatusAndReturnOld(any(), any(), any(),
        any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
  }

  @Test
  void rejectTransactions_NotFoundBatch() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";

    TransactionsRequest transactionsRequest = TransactionsRequest.builder()
        .transactionIds(List.of("trxId")).build();

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.empty());

    Mono<RewardBatch> resultMono = rewardBatchService.rejectTransactions(batchId, initiativeId,
        transactionsRequest);
    Assertions.assertThrows(ClientExceptionWithBody.class, resultMono::block);

    verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(),
        any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(),
        anyLong());

  }

  @Test
  void rejectTransactions_ErrorInUpdateInModifyTrx() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
        String batchMonth = "dicembre 2025";

    TransactionsRequest transactionsRequest = TransactionsRequest.builder()
        .transactionIds(List.of("trxId")).build();

    RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING)
        .month(batchMonth).build();
    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(rewardBatch));

    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

    Mono<RewardBatch> resultMono = rewardBatchService.rejectTransactions(batchId, initiativeId,
        transactionsRequest);
    Assertions.assertThrows(RuntimeException.class, resultMono::block);

    verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(),
        anyLong());
  }

  @Test
  void rejectTransactions_ErrorInUpdateBatch() {
    String batchId = "BATCH_ID";
    String initiativeId = "INITIATIVE_ID";
        String batchMonth = "dicembre 2025";

    TransactionsRequest transactionsRequest = TransactionsRequest.builder()
        .transactionIds(List.of("trxId")).build();

    RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING)
        .month(batchMonth).build();
    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
        .thenReturn(Mono.just(rewardBatch));

    //Mock for approved
    Reward reward = Reward.builder().accruedRewardCents(1000L).build();
    Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
    RewardTransaction trxMock = RewardTransaction.builder()
        .id("trxId")
        .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
        .rewardBatchId(batchId)
        .rewards(rewardApprovedMap).build();
    when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",
        RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason(), batchMonth))
        .thenReturn(Mono.just(trxMock));

    when(rewardBatchRepository.updateTotals(batchId, 1L, 0L, 0, 0))
        .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

    Mono<RewardBatch> resultMono = rewardBatchService.rejectTransactions(batchId, initiativeId,
        transactionsRequest);
    Assertions.assertThrows(RuntimeException.class, resultMono::block);

    verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
  }


  @Test
  void evaluatingRewardBatches() {
    String batchId = "BATCH_ID";
    Long suspendedAmount = 0L;
    RewardBatch rewardBatch = RewardBatch.builder()
        .id(batchId)
        .initialAmountCents(100L)
        .build();

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT))
        .thenReturn(Mono.just(rewardBatch));

    Void voidMock = mock(Void.class);
    when(rewardTransactionRepository.rewardTransactionsByBatchId(batchId))
        .thenReturn(Mono.just(voidMock));

    when(rewardTransactionRepository.sumSuspendedAccruedRewardCents(batchId))
            .thenReturn(Mono.just(suspendedAmount));
    when(rewardBatchRepository.updateStatusAndApprovedAmountCents(batchId,
        RewardBatchStatus.EVALUATING, 100L))
        .thenReturn(Mono.just(rewardBatch));

    Long result = rewardBatchService.evaluatingRewardBatches(List.of(batchId)).block();

    assertNotNull(result);
    Assertions.assertEquals(1L, result);
  }

  @Test
  void evaluatingRewardBatches_notSent() {
    String batchId = "BATCH_ID";

    when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT))
        .thenReturn(Mono.empty());

    Long result = rewardBatchService.evaluatingRewardBatches(List.of(batchId)).block();

    assertNotNull(result);
    assertEquals(0L, result);

    verify(rewardBatchRepository).findByIdAndStatus(any(), any());
    verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
    verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), any());
  }

  @Test
  void evaluatingRewardBatches_emptyList() {
    Long result = rewardBatchService.evaluatingRewardBatches(new ArrayList<>()).block();

    assertNotNull(result);
    assertEquals(0L, result);

    verify(rewardBatchRepository, never()).findByStatus(any());
    verify(rewardBatchRepository, never()).findByIdAndStatus(any(), any());
    verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
    verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), any());
  }

  @Test
  void evaluatingRewardBatches_nullList() {
    String batchId = "BATCH_ID_1";
    Long suspendedAmount = 5L;
    RewardBatch rewardBatch = RewardBatch.builder()
        .id(batchId)
        .initialAmountCents(100L)
        .build();

    when(rewardBatchRepository.findByStatus(RewardBatchStatus.SENT))
        .thenReturn(Flux.just(rewardBatch));

    Void voidMock = mock(Void.class);
    when(rewardTransactionRepository.rewardTransactionsByBatchId(batchId))
        .thenReturn(Mono.just(voidMock));

    when(rewardTransactionRepository.sumSuspendedAccruedRewardCents(batchId))
            .thenReturn(Mono.just(suspendedAmount));

    when(rewardBatchRepository.updateStatusAndApprovedAmountCents(batchId,
        RewardBatchStatus.EVALUATING, rewardBatch.getInitialAmountCents()-suspendedAmount))
        .thenReturn(Mono.just(rewardBatch));

    Long result = rewardBatchService.evaluatingRewardBatches(null).block();

    assertNotNull(result);
    Assertions.assertEquals(1L, result);
  }

  @Test
  void validateRewardBatch_L1ToL2_Success() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactions(100L)
        .numberOfTransactionsElaborated(20L)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));
    when(rewardBatchRepository.save(any())).thenAnswer(
        invocation -> Mono.just(invocation.getArgument(0)));

        RewardBatch result = rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, REWARD_BATCH_ID_1).block();

        assertNotNull(result);
        assertEquals(batch, result);

    verify(rewardBatchRepository, times(1)).save(batch);
    assertEquals(RewardBatchAssignee.L2, batch.getAssigneeLevel());
  }

  @Test
  void validateRewardBatch_L2ToL3_Success() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));
    when(rewardBatchRepository.save(any())).thenAnswer(
        invocation -> Mono.just(invocation.getArgument(0)));

        RewardBatch result = rewardBatchService.validateRewardBatch("operator2", INITIATIVE_ID, REWARD_BATCH_ID_1).block();

        assertNotNull(result);
        assertEquals(batch, result);

    verify(rewardBatchRepository, times(1)).save(batch);
    assertEquals(RewardBatchAssignee.L3, batch.getAssigneeLevel());
  }

  @Test
  void validateRewardBatch_RoleNotAllowed_L1() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactions(100L)
        .numberOfTransactionsElaborated(20L)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.validateRewardBatch("wrongRole", INITIATIVE_ID, REWARD_BATCH_ID_1))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RoleNotAllowedForL1PromotionException.class, ex);
                    assertEquals(ROLE_NOT_ALLOWED_FOR_L1_PROMOTION,
                            ((RoleNotAllowedForL1PromotionException) ex).getCode());
                })
                .verify();
    }

  @Test
  void validateRewardBatch_LessThan15Percent() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactions(100L)
        .numberOfTransactionsElaborated(10L)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, REWARD_BATCH_ID_1))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(BatchNotElaborated15PercentException.class, ex);
                    assertEquals(BATCH_NOT_ELABORATED_15_PERCENT,
                            ((BatchNotElaborated15PercentException) ex).getCode());
                })
                .verify();
    }

  @Test
  void validateRewardBatch_RoleNotAllowed_L2() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.validateRewardBatch("wrongRole", INITIATIVE_ID, REWARD_BATCH_ID_1))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RoleNotAllowedForL2PromotionException.class, ex);
                    assertEquals(ROLE_NOT_ALLOWED_FOR_L2_PROMOTION,
                            ((RoleNotAllowedForL2PromotionException) ex).getCode());
                })
                .verify();
    }

  @Test
  void validateRewardBatch_InvalidState() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L3)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.validateRewardBatch("operator3", INITIATIVE_ID, REWARD_BATCH_ID_1))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(InvalidBatchStateForPromotionException.class, ex);
                    assertEquals(INVALID_BATCH_STATE_FOR_PROMOTION,
                            ((InvalidBatchStateForPromotionException) ex).getCode());
                })
                .verify();
    }

  @Test
  void validateRewardBatch_NotFound() {
    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.empty());

        StepVerifier.create(rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, REWARD_BATCH_ID_1))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchNotFound.class, ex);
                    assertEquals(REWARD_BATCH_NOT_FOUND,
                            ((RewardBatchNotFound) ex).getCode());
                })
                .verify();
    }

  @Test
  void validateRewardBatch_TotalZero() {
    RewardBatch batch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, REWARD_BATCH_ID_1))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(BatchNotElaborated15PercentException.class, ex);
                    assertEquals(BATCH_NOT_ELABORATED_15_PERCENT,
                            ((BatchNotElaborated15PercentException) ex).getCode());
                })
                .verify();
    }

  @Test
  void evaluatingRewardBatchStatusScheduler() {
    when(rewardBatchRepository.findByStatus(RewardBatchStatus.SENT))
        .thenReturn(Flux.empty());

    rewardBatchServiceSpy.evaluatingRewardBatchStatusScheduler();

    verify(rewardBatchRepository).findByStatus(any());
    verify(rewardBatchRepository, never()).findByIdAndStatus(any(), any());
    verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
    verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), any());

    }

    @Test
    void downloadRewardBatch_NotFound() {
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, REWARD_BATCH_ID_1))
                .thenReturn(Mono.empty());

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                MERCHANT_ID,
                                VALID_ROLE,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchNotFound.class, ex);
                    RewardBatchNotFound e = (RewardBatchNotFound) ex;
                    assertEquals(REWARD_BATCH_NOT_FOUND, e.getCode());
                })
                .verify();
    }

  @Test
  void downloadRewardBatch_notMerchantAndOperator() {
    Mono<DownloadRewardBatchResponseDTO> monoResult = rewardBatchService.downloadApprovedRewardBatchFile(
        null,
        null,
        INITIATIVE_ID,
        REWARD_BATCH_ID_1
    );

    RewardBatchInvalidRequestException result = assertThrows(RewardBatchInvalidRequestException.class, monoResult::block);

    assertEquals(REWARD_BATCH_INVALID_REQUEST, result.getCode());
    assertEquals(MERCHANT_OR_OPERATOR_HEADER_MANDATORY, result.getMessage());
  }

    @Test
    void downloadRewardBatch_NotApproved() {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                MERCHANT_ID,
                                VALID_ROLE,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchNotApprovedException.class, ex);
                    RewardBatchNotApprovedException e = (RewardBatchNotApprovedException) ex;
                    assertEquals(REWARD_BATCH_NOT_APPROVED, e.getCode());
                })
                .verify();
    }

    @Test
    void downloadRewardBatch_Success() {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.APPROVED)
                .filename("file.csv")
                .build();

        String blobPath = String.format("initiative/%s/merchant/%s/batch/%s/%s",
                INITIATIVE_ID, MERCHANT_ID, REWARD_BATCH_ID_1, batch.getFilename());

        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));
        when(approvedRewardBatchBlobService.getFileSignedUrl(blobPath))
                .thenReturn("signed-url");

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                MERCHANT_ID,
                                VALID_ROLE,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectNextMatches(response ->
                        response.getApprovedBatchUrl().equals("signed-url"))
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    @NullSource
    void downloadRewardBatch_InvalidFilename(String invalidFilename) {

        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .status(RewardBatchStatus.APPROVED)
                .filename(invalidFilename)
                .build();

        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                MERCHANT_ID,
                                VALID_ROLE,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchMissingFilenameException.class, ex);
                    RewardBatchMissingFilenameException e = (RewardBatchMissingFilenameException) ex;
                    assertEquals(REWARD_BATCH_MISSING_FILENAME, e.getCode());
                })
                .verify();
    }

    @Test
    void downloadRewardBatch_NotFound_WhenMerchantIdNull() {
        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.empty());

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                null,
                                VALID_ROLE,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchNotFound.class, ex);
                    RewardBatchNotFound e = (RewardBatchNotFound) ex;
                    assertEquals(REWARD_BATCH_NOT_FOUND, e.getCode());
                })
                .verify();
    }

    @Test
    void downloadRewardBatch_RoleNotAllowed_WhenMerchantIdNull() {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .status(RewardBatchStatus.APPROVED)
                .filename("file.csv")
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                null,
                                INVALID_ROLE,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectError(RoleNotAllowedException.class)
                .verify();
    }

    @Test
    void downloadRewardBatch_NotApproved_WhenMerchantIdNull() {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        String validRole = "operator1";

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                null,
                                validRole,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectError(RewardBatchNotApprovedException.class)
                .verify();
    }

    @Test
    void downloadRewardBatch_Success_WhenMerchantIdNull() {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .status(RewardBatchStatus.APPROVED)
                .filename("file.csv")
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        String blobPath = String.format(
                "initiative/%s/merchant/%s/batch/%s/%s",
                INITIATIVE_ID,
                null,
                REWARD_BATCH_ID_1,
                batch.getFilename()
        );

        when(approvedRewardBatchBlobService.getFileSignedUrl(blobPath))
                .thenReturn("signed-url");

        String validRole = "operator2";

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                null,
                                validRole,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectNextMatches(response ->
                        "signed-url".equals(response.getApprovedBatchUrl())
                )
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    @NullSource
    void downloadRewardBatch_InvalidFilename_WhenMerchantIdNull(String invalidFilename) {
        RewardBatch batch = RewardBatch.builder()
                .id(REWARD_BATCH_ID_1)
                .status(RewardBatchStatus.APPROVED)
                .filename(invalidFilename)
                .build();

        when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
                .thenReturn(Mono.just(batch));

        String validRole = "operator1";

        StepVerifier.create(
                        rewardBatchService.downloadApprovedRewardBatchFile(
                                null,
                                validRole,
                                INITIATIVE_ID,
                                REWARD_BATCH_ID_1
                        )
                )
                .expectError(RewardBatchMissingFilenameException.class)
                .verify();
    }

  @Test
  void sendRewardBatch_success_physical() {
    YearMonth batchMonth = YearMonth.now().minusMonths(2);

    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(batchMonth.toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    when(rewardBatchRepository.findById("B1")).thenReturn(Mono.just(batch));
    when(rewardBatchRepository.findByMerchantIdAndPosType("M1", PosType.PHYSICAL))
        .thenReturn(Flux.empty());
    when(rewardBatchRepository.save(any()))
        .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .verifyComplete();
  }

  @Test
  void sendRewardBatch_success_online() {
    YearMonth batchMonth = YearMonth.now().minusMonths(2);

    RewardBatch batch = RewardBatch.builder()
        .id("B2")
        .merchantId("M1")
        .month(batchMonth.toString())
        .posType(PosType.ONLINE)
        .status(RewardBatchStatus.CREATED)
        .build();

    when(rewardBatchRepository.findById("B2")).thenReturn(Mono.just(batch));
    when(rewardBatchRepository.findByMerchantIdAndPosType("M1", PosType.ONLINE))
        .thenReturn(Flux.empty());
    when(rewardBatchRepository.save(any()))
        .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B2"))
        .verifyComplete();
  }
  @Test
  void sendRewardBatch_merchantMismatch() {
    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month("2025-10")
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    when(rewardBatchRepository.findById("B1")).thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M2", "B1"))
        .expectErrorSatisfies(ex -> {
          assertInstanceOf(RewardBatchException.class, ex);
          assertTrue(ex.getMessage().contains("REWARD_BATCH_NOT_FOUND"));
        })
        .verify();
  }

  @Test
  void sendRewardBatch_previousNotSent() {
    YearMonth now = YearMonth.now();

    YearMonth batchMonth = now.minusMonths(1);

    RewardBatch batch = RewardBatch.builder()
        .id("B2")
        .merchantId("M1")
        .month(batchMonth.toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    RewardBatch previous = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(batchMonth.minusMonths(1).toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    when(rewardBatchRepository.findById("B2")).thenReturn(Mono.just(batch));
    when(rewardBatchRepository.findByMerchantIdAndPosType("M1", PosType.PHYSICAL))
        .thenReturn(Flux.just(previous));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B2"))
        .expectErrorSatisfies(ex -> {
          assertInstanceOf(RewardBatchException.class, ex);
          assertTrue(ex.getMessage().contains("REWARD_BATCH_PREVIOUS_NOT_SENT"));
        })
        .verify();
  }

  @Test
  void sendRewardBatch_allPreviousSent_success() {
    YearMonth now = YearMonth.now();

    RewardBatch batch = RewardBatch.builder()
        .id("B2")
        .merchantId("M1")
        .month(now.minusMonths(1).toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .build();

    RewardBatch previous = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(now.minusMonths(2).toString())
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.SENT)
        .build();

    when(rewardBatchRepository.findById("B2")).thenReturn(Mono.just(batch));
    when(rewardBatchRepository.findByMerchantIdAndPosType("M1", PosType.PHYSICAL))
        .thenReturn(Flux.just(previous));
    when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B2"))
        .verifyComplete();
  }

  @Test
  void postponeTransaction_createsNewBatchAndMovesTransaction() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.of(2026, 1, 6);

    RewardTransaction trx = RewardTransaction.builder()
        .id(transactionId)
        .merchantId("M1")
        .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
        .rewardBatchId(REWARD_BATCH_ID_1)
        .build();

    when(rewardTransactionRepository.findTransactionInBatch("M1", REWARD_BATCH_ID_1, transactionId))
        .thenReturn(Mono.just(trx));

    RewardBatch currentBatch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .merchantId("M1")
        .month("2026-01")
        .status(RewardBatchStatus.CREATED)
        .posType(PosType.PHYSICAL)
        .businessName(BUSINESS_NAME)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(currentBatch));

    RewardBatch newBatch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_2)
        .merchantId("M1")
        .month("2026-02")
        .status(RewardBatchStatus.CREATED)
        .posType(PosType.PHYSICAL)
        .businessName(BUSINESS_NAME)
        .build();

    doReturn(Mono.just(newBatch))
        .when(rewardBatchServiceSpy)
        .findOrCreateBatch("M1", PosType.PHYSICAL, "2026-02", BUSINESS_NAME);

    doReturn(Mono.just(currentBatch)).when(rewardBatchServiceSpy).decrementTotals(REWARD_BATCH_ID_1, 100L);
    doReturn(Mono.just(newBatch)).when(rewardBatchServiceSpy).incrementTotals(REWARD_BATCH_ID_2, 100L);

    when(rewardTransactionRepository.save(any(RewardTransaction.class))).thenReturn(Mono.just(trx));

    StepVerifier.create(rewardBatchServiceSpy.postponeTransaction(
            "M1", INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId, initiativeEndDate))
        .verifyComplete();

    verify(rewardTransactionRepository, times(1)).findTransactionInBatch("M1", REWARD_BATCH_ID_1, transactionId);
    verify(rewardBatchRepository, times(1)).findById(REWARD_BATCH_ID_1);
    verify(rewardBatchServiceSpy, times(1))
        .findOrCreateBatch("M1", PosType.PHYSICAL, "2026-02", BUSINESS_NAME);
    verify(rewardBatchServiceSpy, times(1)).decrementTotals(REWARD_BATCH_ID_1, 100L);
    verify(rewardBatchServiceSpy, times(1)).incrementTotals(REWARD_BATCH_ID_2, 100L);
    verify(rewardTransactionRepository, times(1)).save(trx);

    assertEquals(REWARD_BATCH_ID_2, trx.getRewardBatchId());
    assertNotNull(trx.getRewardBatchInclusionDate());
  }

  @Test
  void postponeTransaction_invalidBatchStatus() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.of(2026, 1, 6);

    RewardTransaction trx = RewardTransaction.builder()
        .id(transactionId)
        .merchantId("M1")
        .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
        .rewardBatchId(REWARD_BATCH_ID_1)
        .build();

    RewardBatch currentBatch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .month(CURRENT_MONTH)
        .status(RewardBatchStatus.SENT)
        .posType(PosType.PHYSICAL)
        .merchantId("M1")
        .businessName(BUSINESS_NAME)
        .build();

    when(rewardTransactionRepository.findTransactionInBatch("M1", REWARD_BATCH_ID_1, transactionId))
        .thenReturn(Mono.just(trx));
    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(currentBatch));

    StepVerifier.create(rewardBatchService.postponeTransaction(
            "M1", INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId, initiativeEndDate))
        .expectErrorSatisfies(ex -> {
          assertInstanceOf(ClientExceptionWithBody.class, ex);
          ClientExceptionWithBody ce = (ClientExceptionWithBody) ex;
          assertEquals(ExceptionCode.REWARD_BATCH_INVALID_REQUEST, ce.getCode());
          assertEquals(ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH, ce.getMessage());
        })
        .verify();
  }

  @Test
  void postponeTransaction_beyondAllowedPostponeMonth() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.of(2026, 1, 6);

    RewardTransaction trx = RewardTransaction.builder()
        .id(transactionId)
        .merchantId("M1")
        .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
        .rewardBatchId(REWARD_BATCH_ID_1)
        .build();

    RewardBatch currentBatch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .month("2026-02")
        .status(RewardBatchStatus.CREATED)
        .posType(PosType.PHYSICAL)
        .merchantId("M1")
        .businessName(BUSINESS_NAME)
        .build();

    when(rewardTransactionRepository.findTransactionInBatch("M1", REWARD_BATCH_ID_1, transactionId))
        .thenReturn(Mono.just(trx));
    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1)).thenReturn(Mono.just(currentBatch));

    StepVerifier.create(rewardBatchService.postponeTransaction(
            "M1", INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId, initiativeEndDate))
        .expectErrorSatisfies(ex -> {
          assertInstanceOf(ClientExceptionWithBody.class, ex);
          ClientExceptionWithBody ce = (ClientExceptionWithBody) ex;
          assertEquals(ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED, ce.getCode());
          assertEquals(ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED, ce.getMessage());
        })
        .verify();
  }

  @Test
  void postponeTransaction_destinationBatchNotCreated() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.of(2026, 1, 6);

    RewardTransaction trx = RewardTransaction.builder()
        .id(transactionId)
        .merchantId("M1")
        .rewards(Map.of(INITIATIVE_ID,
            Reward.builder().accruedRewardCents(100L).build()))
        .rewardBatchId(REWARD_BATCH_ID_1)
        .build();

    when(rewardTransactionRepository
        .findTransactionInBatch("M1", REWARD_BATCH_ID_1, transactionId))
        .thenReturn(Mono.just(trx));

    RewardBatch currentBatch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_1)
        .merchantId("M1")
        .month("2026-01")
        .status(RewardBatchStatus.CREATED)
        .posType(PosType.PHYSICAL)
        .businessName(BUSINESS_NAME)
        .build();

    when(rewardBatchRepository.findById(REWARD_BATCH_ID_1))
        .thenReturn(Mono.just(currentBatch));

    RewardBatch nextBatch = RewardBatch.builder()
        .id(REWARD_BATCH_ID_2)
        .merchantId("M1")
        .month("2026-02")
        .status(RewardBatchStatus.APPROVED)
        .posType(PosType.PHYSICAL)
        .businessName(BUSINESS_NAME)
        .build();

    doReturn(Mono.just(nextBatch))
        .when(rewardBatchServiceSpy)
        .findOrCreateBatch("M1", PosType.PHYSICAL, "2026-02", BUSINESS_NAME);

    StepVerifier.create(
            rewardBatchServiceSpy.postponeTransaction(
                "M1",
                INITIATIVE_ID,
                REWARD_BATCH_ID_1,
                transactionId,
                initiativeEndDate
            ))
        .expectErrorSatisfies(ex -> {
          assertInstanceOf(ClientExceptionNoBody.class, ex);
          ClientExceptionNoBody ce = (ClientExceptionNoBody) ex;
          assertEquals(ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH, ce.getMessage());
        })
        .verify();

    verify(rewardBatchServiceSpy, never())
        .decrementTotals(anyString(), anyLong());
    verify(rewardBatchServiceSpy, never())
        .incrementTotals(anyString(), anyLong());
    verify(rewardTransactionRepository, never()).save(any());
  }

  @Test
  void approveTransaction_whenTransactionSuspendedInPreviousBatch(){

      String initiativeId = "INITIATIVE_ID";
      RewardBatch actualBatch = RewardBatch.builder().id("BATCH_ID").month("dicembre 2025").build();
      when(rewardBatchRepository.findByIdAndStatus(actualBatch.getId(), RewardBatchStatus.EVALUATING))
              .thenReturn(Mono.just(actualBatch));

      Reward reward = Reward.builder().accruedRewardCents(1000L).build();
      Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
      RewardTransaction trxMock = RewardTransaction.builder()
              .id("trxId")
              .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
              .rewardBatchId(actualBatch.getId())
              .rewards(rewardApprovedMap)
              .rewardBatchLastMonthElaborated("novembre 2025").build();

      TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of(trxMock.getId())).build();


      when(rewardTransactionRepository.updateStatusAndReturnOld(actualBatch.getId(), trxMock.getId(),
              RewardBatchTrxStatus.APPROVED, null, actualBatch.getMonth()))
              .thenReturn(Mono.just(trxMock));


      RewardBatch expectedResult = new RewardBatch();
      when(rewardBatchRepository.updateTotals(
              actualBatch.getId(),
              1L,
              trxMock.getRewards().get(initiativeId).getAccruedRewardCents(),
              0L,
              -1L))
              .thenReturn(Mono.just(expectedResult));

      RewardBatch result = rewardBatchService.approvedTransactions(actualBatch.getId(), request, initiativeId).block();

      Assertions.assertNotNull(result);
      Assertions.assertEquals(expectedResult, result);

      verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
      verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
  }

    @Test
    void approveTransaction_whenTransactionSuspendedInActualBatch(){

        String initiativeId = "INITIATIVE_ID";
        RewardBatch actualBatch = RewardBatch.builder().id("BATCH_ID").month("dicembre 2025").build();
        when(rewardBatchRepository.findByIdAndStatus(actualBatch.getId(), RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(actualBatch));

        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(actualBatch.getId())
                .rewards(rewardApprovedMap)
                .rewardBatchLastMonthElaborated(actualBatch.getMonth()).build();

        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of(trxMock.getId())).build();


        when(rewardTransactionRepository.updateStatusAndReturnOld(actualBatch.getId(), trxMock.getId(),
                RewardBatchTrxStatus.APPROVED, null, actualBatch.getMonth()))
                .thenReturn(Mono.just(trxMock));


        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                actualBatch.getId(),
                0L,
                trxMock.getRewards().get(initiativeId).getAccruedRewardCents(),
                0L,
                -1L))
                .thenReturn(Mono.just(expectedResult));

        RewardBatch result = rewardBatchService.approvedTransactions(actualBatch.getId(), request, initiativeId).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
        verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void rejectTransaction_whenTransactionSuspendedInPreviousBatch(){

        String initiativeId = "INITIATIVE_ID";
        RewardBatch actualBatch = RewardBatch.builder().id("BATCH_ID").month("dicembre 2025").build();
        when(rewardBatchRepository.findByIdAndStatus(actualBatch.getId(), RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(actualBatch));

        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(actualBatch.getId())
                .rewards(rewardApprovedMap)
                .rewardBatchLastMonthElaborated("novembre 2025").build();

        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of(trxMock.getId())).reason("REASON").build();


        when(rewardTransactionRepository.updateStatusAndReturnOld(actualBatch.getId(), trxMock.getId(),
                RewardBatchTrxStatus.REJECTED, request.getReason(), actualBatch.getMonth()))
                .thenReturn(Mono.just(trxMock));


        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                actualBatch.getId(),
                1L,
                0L,
                1L,
                -1L))
                .thenReturn(Mono.just(expectedResult));

        RewardBatch result = rewardBatchService.rejectTransactions(actualBatch.getId(), initiativeId, request).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
        verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void rejectTransaction_whenTransactionSuspendedInActualBatch(){

        String initiativeId = "INITIATIVE_ID";
        RewardBatch actualBatch = RewardBatch.builder().id("BATCH_ID").month("dicembre 2025").build();
        when(rewardBatchRepository.findByIdAndStatus(actualBatch.getId(), RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(actualBatch));

        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(actualBatch.getId())
                .rewards(rewardApprovedMap)
                .rewardBatchLastMonthElaborated(actualBatch.getMonth()).build();

        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of(trxMock.getId())).reason("REASON").build();


        when(rewardTransactionRepository.updateStatusAndReturnOld(actualBatch.getId(), trxMock.getId(),
                RewardBatchTrxStatus.REJECTED, request.getReason(), actualBatch.getMonth()))
                .thenReturn(Mono.just(trxMock));


        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                actualBatch.getId(),
                0L,
                0L,
                1L,
                -1L))
                .thenReturn(Mono.just(expectedResult));

        RewardBatch result = rewardBatchService.rejectTransactions(actualBatch.getId(), initiativeId, request).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
        verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void suspendTransaction_whenTransactionSuspendedInPreviousBatch(){

        String initiativeId = "INITIATIVE_ID";
        RewardBatch actualBatch = RewardBatch.builder().id("BATCH_ID").month("dicembre 2025").build();
        when(rewardBatchRepository.findByIdAndStatus(actualBatch.getId(), RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(actualBatch));

        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(actualBatch.getId())
                .rewards(rewardApprovedMap)
                .rewardBatchLastMonthElaborated("novembre 2025").build();

        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of(trxMock.getId())).reason("REASON").build();


        when(rewardTransactionRepository.updateStatusAndReturnOld(actualBatch.getId(), trxMock.getId(),
                RewardBatchTrxStatus.SUSPENDED, request.getReason(), actualBatch.getMonth()))
                .thenReturn(Mono.just(trxMock));


        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                actualBatch.getId(),
                1L,
                0L,
                0L,
                0L))
                .thenReturn(Mono.just(expectedResult));

        RewardBatch result = rewardBatchService.suspendTransactions(actualBatch.getId(), initiativeId, request).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
        verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void suspendTransaction_whenTransactionSuspendedInActualBatch(){

        String initiativeId = "INITIATIVE_ID";
        RewardBatch actualBatch = RewardBatch.builder().id("BATCH_ID").month("dicembre 2025").build();
        when(rewardBatchRepository.findByIdAndStatus(actualBatch.getId(), RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(actualBatch));

        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(actualBatch.getId())
                .rewards(rewardApprovedMap)
                .rewardBatchLastMonthElaborated("dicembre 2025").build();

        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of(trxMock.getId())).reason("REASON").build();


        when(rewardTransactionRepository.updateStatusAndReturnOld(actualBatch.getId(), trxMock.getId(),
                RewardBatchTrxStatus.SUSPENDED, request.getReason(), actualBatch.getMonth()))
                .thenReturn(Mono.just(trxMock));


        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                actualBatch.getId(),
                0L,
                0L,
                0L,
                0L))
                .thenReturn(Mono.just(expectedResult));

        RewardBatch result = rewardBatchService.suspendTransactions(actualBatch.getId(), initiativeId, request).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(), any(), any(), any());
        verify(rewardBatchRepository).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

}

