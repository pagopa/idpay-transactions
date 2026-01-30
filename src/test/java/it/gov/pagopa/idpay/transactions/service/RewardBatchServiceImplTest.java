package it.gov.pagopa.idpay.transactions.service;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;
import com.mongodb.client.result.DeleteResult;
import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.*;
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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private UserRestClient userRestClient;
    @Mock private ApprovedRewardBatchBlobService approvedRewardBatchBlobService;
    @Mock private ReactiveMongoTemplate reactiveMongoTemplate;
    @Mock private ChecksErrorMapper checksErrorMapper;
    @Mock private AuditUtilities auditUtilities;

    private RewardBatchServiceImpl service;
    private RewardBatchServiceImpl serviceSpy;

    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String BUSINESS_NAME = "Business";
    private static final String BATCH_ID = "BATCH_ID";
    private static final String BATCH_ID_2 = "BATCH_ID_2";
    private static final String OP1 = "operator1";
    private static final String OP2 = "operator2";
    private static final String OP3 = "operator3";

    @BeforeEach
    void setup() {
        service = new RewardBatchServiceImpl(
                rewardBatchRepository,
                rewardTransactionRepository,
                userRestClient,
                approvedRewardBatchBlobService,
                reactiveMongoTemplate,
                checksErrorMapper,
                auditUtilities
        );
        serviceSpy = spy(service);
    }

    @Test
    void findOrCreateBatch_returnsExisting() {
        RewardBatch existing = RewardBatch.builder()
                .id("EX")
                .merchantId("M1")
                .posType(PHYSICAL)
                .month("2025-11")
                .status(RewardBatchStatus.CREATED)
                .name("novembre 2025")
                .build();

        when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.just(existing));

        StepVerifier.create(service.findOrCreateBatch("M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(any());
    }

    @Test
    void findOrCreateBatch_createsNew_whenMissing() {
        when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.empty());

        when(rewardBatchRepository.save(any()))
                .thenAnswer(inv -> {
                    RewardBatch b = inv.getArgument(0);
                    b.setId("NEW");
                    return Mono.just(b);
                });

        StepVerifier.create(service.findOrCreateBatch("M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .assertNext(b -> {
                    assertEquals("NEW", b.getId());
                    assertEquals("M1", b.getMerchantId());
                    assertEquals(PHYSICAL, b.getPosType());
                    assertEquals("2025-11", b.getMonth());
                    assertEquals(RewardBatchStatus.CREATED, b.getStatus());
                    assertTrue(b.getName().contains("novembre"));
                    assertNotNull(b.getStartDate());
                    assertNotNull(b.getEndDate());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(any());
    }

    @Test
    void findOrCreateBatch_duplicateKey_fallbackFind() {
        RewardBatch existing = RewardBatch.builder()
                .id("DUP")
                .merchantId("M1")
                .posType(PHYSICAL)
                .month("2025-11")
                .status(RewardBatchStatus.CREATED)
                .name("novembre 2025")
                .build();

        when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.empty())
                .thenReturn(Mono.just(existing));

        when(rewardBatchRepository.save(any()))
                .thenReturn(Mono.error(new DuplicateKeyException("dup")));

        StepVerifier.create(service.findOrCreateBatch("M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository).save(any());
        verify(rewardBatchRepository, times(2)).findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11");
    }

    @Test
    void isOperator_privateRoleChecks() throws Exception {
        Method m = RewardBatchServiceImpl.class.getDeclaredMethod("isOperator", String.class);
        m.setAccessible(true);

        assertFalse((boolean) m.invoke(service, (String) null));
        assertFalse((boolean) m.invoke(service, "guest"));
        assertTrue((boolean) m.invoke(service, "operator1"));
        assertTrue((boolean) m.invoke(service, "OPERATOR2"));
    }

    @Test
    void getRewardBatches_operatorVsMerchant() {
        PageRequest pageable = PageRequest.of(0, 2);

        RewardBatch b1 = RewardBatch.builder().id("B1").merchantId("M1").build();
        RewardBatch b2 = RewardBatch.builder().id("B2").merchantId("M2").build();

        when(rewardBatchRepository.findRewardBatchesCombined(null, null, null, null, true, pageable))
                .thenReturn(Flux.just(b1, b2));
        when(rewardBatchRepository.getCountCombined(null, null, null, null, true))
                .thenReturn(Mono.just(10L));

        StepVerifier.create(service.getRewardBatches(null, "operator1", null, null, null, pageable))
                .assertNext(p -> {
                    assertEquals(2, p.getContent().size());
                    assertEquals(10L, p.getTotalElements());
                })
                .verifyComplete();

        when(rewardBatchRepository.findRewardBatchesCombined("M1", null, null, null, false, pageable))
                .thenReturn(Flux.just(b1));
        when(rewardBatchRepository.getCountCombined("M1", null, null, null, false))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getRewardBatches("M1", "guest", null, null, null, pageable))
                .assertNext(p -> {
                    assertEquals(1, p.getContent().size());
                    assertEquals(1L, p.getTotalElements());
                })
                .verifyComplete();
    }


    @Test
    void incrementDecrementMoveSuspend_callsRepository() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).initialAmountCents(100L).build();

        when(rewardBatchRepository.incrementTotals(BATCH_ID, 50L)).thenReturn(Mono.just(rb));
        when(rewardBatchRepository.decrementTotals(BATCH_ID, 10L)).thenReturn(Mono.just(rb));
        when(rewardBatchRepository.moveSuspendToNewBatch("OLD", "NEW", 99L)).thenReturn(Mono.just(rb));

        StepVerifier.create(service.incrementTotals(BATCH_ID, 50L)).expectNext(rb).verifyComplete();
        StepVerifier.create(service.decrementTotals(BATCH_ID, 10L)).expectNext(rb).verifyComplete();
        StepVerifier.create(service.moveSuspendToNewBatch("OLD", "NEW", 99L)).expectNext(rb).verifyComplete();
    }

    @Test
    void sendRewardBatch_batchNotFound() {
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.sendRewardBatch(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_merchantMismatch() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId("OTHER").status(RewardBatchStatus.CREATED).month("2025-01").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.sendRewardBatch(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_invalidStatus() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.SENT).month("2025-01").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.sendRewardBatch(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_monthTooEarly() {
        YearMonth now = YearMonth.now();
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.CREATED).month(now.toString()).posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.sendRewardBatch(MERCHANT_ID, BATCH_ID))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchException.class, ex);
                    assertTrue(ex.getMessage().contains("REWARD_BATCH_MONTH_TOO_EARLY"));
                })
                .verify();
    }

    @Test
    void sendRewardBatch_previousNotSent() {
        YearMonth batchMonth = YearMonth.now().minusMonths(1);

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.CREATED)
                .month(batchMonth.toString())
                .posType(PHYSICAL)
                .build();

        RewardBatch previousCreated = RewardBatch.builder()
                .id("PREV")
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.CREATED)
                .month(batchMonth.minusMonths(1).toString())
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndPosType(MERCHANT_ID, PHYSICAL))
                .thenReturn(Flux.just(previousCreated));

        StepVerifier.create(service.sendRewardBatch(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_success_allPreviousSent() {
        YearMonth batchMonth = YearMonth.now().minusMonths(1);

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.CREATED)
                .month(batchMonth.toString())
                .posType(PHYSICAL)
                .build();

        RewardBatch previousSent = RewardBatch.builder()
                .id("PREV")
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.SENT)
                .month(batchMonth.minusMonths(1).toString())
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndPosType(MERCHANT_ID, PHYSICAL))
                .thenReturn(Flux.just(previousSent));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.sendRewardBatch(MERCHANT_ID, BATCH_ID))
                .verifyComplete();

        verify(rewardBatchRepository).save(argThat(b -> b.getStatus() == RewardBatchStatus.SENT && b.getMerchantSendDate() != null));
    }


    @Test
    void validChecksError_null_ok() {
        assertDoesNotThrow(() -> serviceSpy.validChecksError(null));
    }

    @Test
    void validChecksError_allFalse_throws() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(false);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(false);
        dto.setPriceError(false);
        dto.setBonusError(false);
        dto.setSellerReferenceError(false);
        dto.setAccountingDocumentError(false);

        InvalidChecksErrorException ex = assertThrows(InvalidChecksErrorException.class, () -> serviceSpy.validChecksError(dto));
        assertEquals(ERROR_MESSAGE_INVALID_CHECKS_ERROR, ex.getMessage());
    }

    @Test
    void validChecksError_anyTrue_ok() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        assertDoesNotThrow(() -> serviceSpy.validChecksError(dto));
    }

    @Test
    void validChecksError_nullDto_doesNothing() {
        assertDoesNotThrow(() -> service.validChecksError(null));
    }

    @Test
    void validChecksError_allFalse_throwsException() {
        ChecksErrorDTO dto = new ChecksErrorDTO();

        InvalidChecksErrorException ex =
                assertThrows(InvalidChecksErrorException.class,
                        () -> service.validChecksError(dto));

        assertEquals(ERROR_MESSAGE_INVALID_CHECKS_ERROR, ex.getMessage());
    }

    @Test
    void validChecksError_productEligibilityError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setProductEligibilityError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void validChecksError_disposalRaeeError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setDisposalRaeeError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void validChecksError_priceError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setPriceError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void validChecksError_bonusError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setBonusError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void validChecksError_sellerReferenceError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setSellerReferenceError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void validChecksError_accountingDocumentError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setAccountingDocumentError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void validChecksError_genericError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setGenericError(true);

        assertDoesNotThrow(() -> service.validChecksError(dto));
    }

    @Test
    void suspendTransactions_batchNotFoundOrInvalidState() {
        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("t1")).checksError(new ChecksErrorDTO(){{
            setCfError(true);
        }}).build();

        when(checksErrorMapper.toModel(any())).thenReturn(new ChecksError());
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.suspendTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(), any(), any(), any());
    }

    @Test
    void suspendTransactions_coversAllBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();

        ChecksErrorDTO checks = new ChecksErrorDTO();
        checks.setCfError(true);

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("SUSP_PREV", "APP", "TO_CHECK", "CONS", "REJ", "NULL_ACC"))
                .reason("REASON")
                .checksError(checks)
                .build();

        ChecksError model = new ChecksError();
        when(checksErrorMapper.toModel(checks)).thenReturn(model);

        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction trxSuspPrev = RewardTransaction.builder()
                .id("SUSP_PREV")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardTransaction trxApproved = RewardTransaction.builder()
                .id("APP")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(200L).build()))
                .build();

        RewardTransaction trxToCheck = RewardTransaction.builder()
                .id("TO_CHECK")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(300L).build()))
                .build();

        RewardTransaction trxConsultable = RewardTransaction.builder()
                .id("CONS")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(400L).build()))
                .build();

        RewardTransaction trxRejected = RewardTransaction.builder()
                .id("REJ")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(500L).build()))
                .build();

        RewardTransaction trxNullAccrued = RewardTransaction.builder()
                .id("NULL_ACC")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of("OTHER", Reward.builder().accruedRewardCents(999L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "SUSP_PREV", RewardBatchTrxStatus.SUSPENDED, "REASON", batchMonth, model))
                .thenReturn(Mono.just(trxSuspPrev));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "APP", RewardBatchTrxStatus.SUSPENDED, "REASON", batchMonth, model))
                .thenReturn(Mono.just(trxApproved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "TO_CHECK", RewardBatchTrxStatus.SUSPENDED, "REASON", batchMonth, model))
                .thenReturn(Mono.just(trxToCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "CONS", RewardBatchTrxStatus.SUSPENDED, "REASON", batchMonth, model))
                .thenReturn(Mono.just(trxConsultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "REJ", RewardBatchTrxStatus.SUSPENDED, "REASON", batchMonth, model))
                .thenReturn(Mono.just(trxRejected));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "NULL_ACC", RewardBatchTrxStatus.SUSPENDED, "REASON", batchMonth, model))
                .thenReturn(Mono.just(trxNullAccrued));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), anyLong(), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(updated));

        StepVerifier.create(service.suspendTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectNext(updated)
                .verifyComplete();

        verify(auditUtilities).logTransactionsStatusChanged(eq(RewardBatchTrxStatus.SUSPENDED.name()), eq(INITIATIVE_ID), anyString(), eq(checks));
        verify(rewardBatchRepository).updateTotals(eq(BATCH_ID), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void suspendTransactions_alreadySuspended_sameMonth_skipsElaboratedIncrement() throws Exception {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();

        ChecksErrorDTO checks = new ChecksErrorDTO();
        checks.setCfError(true);

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("SUSP_SAME"))
                .reason("R")
                .checksError(checks)
                .build();

        ChecksError model = new ChecksError();
        when(checksErrorMapper.toModel(checks)).thenReturn(model);
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction trxSuspSame = RewardTransaction.builder()
                .id("SUSP_SAME")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-12")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "SUSP_SAME", RewardBatchTrxStatus.SUSPENDED, "R", batchMonth, model))
                .thenReturn(Mono.just(trxSuspSame));

        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), anyLong(), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(service.suspendTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void rejectTransactions_allBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("ALREADY_REJ", "APP", "TO_CHECK", "CONS", "SUSP_PREV"))
                .reason("WHY")
                .build();

        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction alreadyRejected = RewardTransaction.builder()
                .id("ALREADY_REJ")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(10L).build()))
                .build();

        RewardTransaction approved = RewardTransaction.builder()
                .id("APP")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(20L).build()))
                .build();

        RewardTransaction toCheck = RewardTransaction.builder()
                .id("TO_CHECK")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(30L).build()))
                .build();

        RewardTransaction consultable = RewardTransaction.builder()
                .id("CONS")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(40L).build()))
                .build();

        RewardTransaction suspendedPrev = RewardTransaction.builder()
                .id("SUSP_PREV")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "ALREADY_REJ", RewardBatchTrxStatus.REJECTED, "WHY", batchMonth, null))
                .thenReturn(Mono.just(alreadyRejected));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "APP", RewardBatchTrxStatus.REJECTED, "WHY", batchMonth, null))
                .thenReturn(Mono.just(approved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "TO_CHECK", RewardBatchTrxStatus.REJECTED, "WHY", batchMonth, null))
                .thenReturn(Mono.just(toCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "CONS", RewardBatchTrxStatus.REJECTED, "WHY", batchMonth, null))
                .thenReturn(Mono.just(consultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "SUSP_PREV", RewardBatchTrxStatus.REJECTED, "WHY", batchMonth, null))
                .thenReturn(Mono.just(suspendedPrev));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), anyLong(), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(updated));

        StepVerifier.create(service.rejectTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectNext(updated)
                .verifyComplete();
    }

    @Test
    void approvedTransactions_allBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("ALREADY_APP", "TO_CHECK", "CONS", "SUSP_PREV", "REJ"))
                .build();

        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction alreadyApproved = RewardTransaction.builder()
                .id("ALREADY_APP")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(10L).build()))
                .build();

        RewardTransaction toCheck = RewardTransaction.builder()
                .id("TO_CHECK")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(20L).build()))
                .build();

        RewardTransaction consultable = RewardTransaction.builder()
                .id("CONS")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(30L).build()))
                .build();

        RewardTransaction suspendedPrev = RewardTransaction.builder()
                .id("SUSP_PREV")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(40L).build()))
                .build();

        RewardTransaction rejected = RewardTransaction.builder()
                .id("REJ")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "ALREADY_APP", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(alreadyApproved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "TO_CHECK", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(toCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "CONS", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(consultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "SUSP_PREV", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(suspendedPrev));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "REJ", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(rejected));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), anyLong(), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(updated));

        StepVerifier.create(service.approvedTransactions(BATCH_ID, req, INITIATIVE_ID))
                .expectNext(updated)
                .verifyComplete();
    }

    @Test
    void approvedTransactions_batchNotFoundOrInvalidState() {
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("t1")).build();

        StepVerifier.create(service.approvedTransactions(BATCH_ID, req, INITIATIVE_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void evaluatingRewardBatches_nullList_processesAllSent() {
        RewardBatch sent = RewardBatch.builder().id("S1").status(RewardBatchStatus.SENT).initialAmountCents(100L).build();

        when(rewardBatchRepository.findByStatus(RewardBatchStatus.SENT)).thenReturn(Flux.just(sent));
        when(rewardTransactionRepository.rewardTransactionsByBatchId("S1")).thenReturn(Mono.empty());
        when(rewardTransactionRepository.sumSuspendedAccruedRewardCents("S1")).thenReturn(Mono.just(20L));
        when(rewardBatchRepository.updateStatusAndApprovedAmountCents("S1", RewardBatchStatus.EVALUATING, 80L))
                .thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(null))
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void evaluatingRewardBatches_withList_handlesMissingIdsAsEmpty() {
        when(rewardBatchRepository.findByIdAndStatus("S1", RewardBatchStatus.SENT)).thenReturn(Mono.empty());

        StepVerifier.create(service.evaluatingRewardBatches(List.of("S1")))
                .expectNext(0L)
                .verifyComplete();

        verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), anyLong());
    }

    @Test
    void evaluatingRewardBatchStatusScheduler_handlesRewardBatchNotFoundError() {
        doReturn(Mono.error(new RewardBatchNotFound(REWARD_BATCH_NOT_FOUND, "x")))
                .when(serviceSpy).evaluatingRewardBatches(null);

        serviceSpy.evaluatingRewardBatchStatusScheduler();

        verify(serviceSpy).evaluatingRewardBatches(null);
    }


    @Test
    void downloadApprovedRewardBatchFile_invalidRequest_missingHeaders() {
        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, null, INITIATIVE_ID, BATCH_ID))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchInvalidRequestException.class, ex);
                    assertEquals(MERCHANT_OR_OPERATOR_HEADER_MANDATORY, ex.getMessage());
                })
                .verify();
    }

    @Test
    void downloadApprovedRewardBatchFile_notFound_merchantPath() {
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.downloadApprovedRewardBatchFile(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchNotFound.class)
                .verify();
    }

    @Test
    void downloadApprovedRewardBatchFile_roleNotAllowed_whenMerchantNull() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, "guest", INITIATIVE_ID, BATCH_ID))
                .expectError(RoleNotAllowedException.class)
                .verify();
    }

    @Test
    void downloadApprovedRewardBatchFile_notApproved() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.downloadApprovedRewardBatchFile(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchNotApprovedException.class)
                .verify();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "   "})
    void downloadApprovedRewardBatchFile_missingFilename(String invalid) {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename(invalid).merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.downloadApprovedRewardBatchFile(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchMissingFilenameException.class)
                .verify();
    }

    @Test
    void downloadApprovedRewardBatchFile_success_merchant() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();

        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));
        when(approvedRewardBatchBlobService.getFileSignedUrl(anyString())).thenReturn("signed");

        StepVerifier.create(service.downloadApprovedRewardBatchFile(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .assertNext(r -> assertEquals("signed", r.getApprovedBatchUrl()))
                .verifyComplete();
    }

    @Test
    void downloadApprovedRewardBatchFile_success_operatorOnlyMerchantNull() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        when(approvedRewardBatchBlobService.getFileSignedUrl(anyString())).thenReturn("signed2");

        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, OP2, INITIATIVE_ID, BATCH_ID))
                .assertNext(r -> assertEquals("signed2", r.getApprovedBatchUrl()))
                .verifyComplete();
    }


    @Test
    void rewardBatchConfirmation_notFound() {
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void rewardBatchConfirmation_invalidState() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void rewardBatchConfirmation_previousNotApproved_blocks() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3)
                .merchantId(MERCHANT_ID).posType(PHYSICAL).month("2025-12").build();

        RewardBatch prev = RewardBatch.builder().id("P1").status(RewardBatchStatus.SENT).build();

        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchRepository.findRewardBatchByMonthBefore(MERCHANT_ID, PHYSICAL, "2025-12"))
                .thenReturn(Flux.just(prev));

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void rewardBatchConfirmation_success() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3)
                .merchantId(MERCHANT_ID).posType(PHYSICAL).month("2025-12").build();

        RewardBatch prevApproved = RewardBatch.builder().id("P1").status(RewardBatchStatus.APPROVED).build();

        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchRepository.findRewardBatchByMonthBefore(MERCHANT_ID, PHYSICAL, "2025-12"))
                .thenReturn(Flux.just(prevApproved));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> {
                    assertEquals(RewardBatchStatus.APPROVING, updated.getStatus());
                    assertNotNull(updated.getApprovalDate());
                    assertNotNull(updated.getUpdateDate());
                })
                .verifyComplete();
    }

    @Test
    void rewardBatchConfirmationBatch_withIds_processesEach() {
        doReturn(Mono.just(RewardBatch.builder().id(BATCH_ID).build()))
                .when(serviceSpy).processSingleBatchSafe(eq(BATCH_ID), anyString());
        doReturn(Mono.just(RewardBatch.builder().id(BATCH_ID_2).build()))
                .when(serviceSpy).processSingleBatchSafe(eq(BATCH_ID_2), anyString());

        StepVerifier.create(serviceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2)))
                .verifyComplete();

        verify(serviceSpy).processSingleBatchSafe(BATCH_ID, INITIATIVE_ID);
        verify(serviceSpy).processSingleBatchSafe(BATCH_ID_2, INITIATIVE_ID);
        verify(rewardBatchRepository, never()).findRewardBatchByStatus(any());
    }

    @Test
    void rewardBatchConfirmationBatch_emptyList_fetchesApprovingAndProcesses() {
        RewardBatch b1 = RewardBatch.builder().id(BATCH_ID).build();
        RewardBatch b2 = RewardBatch.builder().id(BATCH_ID_2).build();

        when(rewardBatchRepository.findRewardBatchByStatus(RewardBatchStatus.APPROVING))
                .thenReturn(Flux.just(b1, b2));

        doReturn(Mono.just(b1)).when(serviceSpy).processSingleBatchSafe(eq(BATCH_ID), anyString());
        doReturn(Mono.just(b2)).when(serviceSpy).processSingleBatchSafe(eq(BATCH_ID_2), anyString());

        StepVerifier.create(serviceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID, Collections.emptyList()))
                .verifyComplete();

        verify(serviceSpy).processSingleBatchSafe(BATCH_ID, INITIATIVE_ID);
        verify(serviceSpy).processSingleBatchSafe(BATCH_ID_2, INITIATIVE_ID);
    }

    @Test
    void processSingleBatchSafe_onError_returnsEmpty() {
        doReturn(Mono.error(new RuntimeException("boom"))).when(serviceSpy).processSingleBatch(eq(BATCH_ID), anyString());

        StepVerifier.create(serviceSpy.processSingleBatchSafe(BATCH_ID, INITIATIVE_ID))
                .verifyComplete();
    }

    @Test
    void processSingleBatch_notFound() {
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.processSingleBatch(BATCH_ID, INITIATIVE_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void processSingleBatch_invalidState() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3).build();
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));

        StepVerifier.create(service.processSingleBatch(BATCH_ID, INITIATIVE_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void processSingleBatch_success_noSuspended_csvOk() {
        RewardBatch rb = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3)
                .numberOfTransactionsSuspended(0L)
                .merchantId(MERCHANT_ID)
                .build();

        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        doReturn(Mono.empty()).when(serviceSpy).updateAndSaveRewardTransactionsToApprove(BATCH_ID, INITIATIVE_ID);
        doReturn(Mono.just("file.csv")).when(serviceSpy).generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID);
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(serviceSpy.processSingleBatch(BATCH_ID, INITIATIVE_ID))
                .assertNext(saved -> {
                    assertEquals(RewardBatchStatus.APPROVED, saved.getStatus());
                    assertNotNull(saved.getUpdateDate());
                })
                .verifyComplete();

        verify(serviceSpy, never()).createRewardBatchAndSave(any());
    }

    @Test
    void processSingleBatch_success_withSuspended_csvFailsButRecovered() {
        RewardBatch rb = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3)
                .numberOfTransactionsSuspended(2L)
                .merchantId(MERCHANT_ID)
                .month("2025-12")
                .name("dicembre 2025")
                .posType(PHYSICAL)
                .businessName(BUSINESS_NAME)
                .build();

        RewardBatch newBatch = RewardBatch.builder()
                .id(BATCH_ID_2)
                .status(RewardBatchStatus.CREATED)
                .initialAmountCents(0L)
                .numberOfTransactions(0L)
                .numberOfTransactionsSuspended(0L)
                .build();

        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        doReturn(Mono.empty()).when(serviceSpy).updateAndSaveRewardTransactionsToApprove(BATCH_ID, INITIATIVE_ID);

        doReturn(Mono.just(newBatch)).when(serviceSpy).createRewardBatchAndSave(any());
        doReturn(Mono.just(300L)).when(serviceSpy).updateAndSaveRewardTransactionsSuspended(eq(BATCH_ID), eq(INITIATIVE_ID), eq(BATCH_ID_2), eq("2025-12"));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        doReturn(Mono.error(new RuntimeException("csv fail"))).when(serviceSpy).generateAndSaveCsv(eq(BATCH_ID), eq(INITIATIVE_ID), anyString());

        StepVerifier.create(serviceSpy.processSingleBatch(BATCH_ID, INITIATIVE_ID))
                .assertNext(saved -> assertEquals(RewardBatchStatus.APPROVED, saved.getStatus()))
                .verifyComplete();

        verify(rewardBatchRepository).save(argThat(b -> BATCH_ID_2.equals(b.getId()) && b.getSuspendedAmountCents() != null));
    }

    @Test
    void handleSuspendedTransactions_nullOrZero_returnsOriginal() throws Exception {
        RewardBatch rbNull = RewardBatch.builder().id(BATCH_ID).numberOfTransactionsSuspended(null).build();
        RewardBatch rbZero = RewardBatch.builder().id(BATCH_ID).numberOfTransactionsSuspended(0L).build();

        Mono<RewardBatch> r1 = ReflectionTestUtils.invokeMethod(serviceSpy, "handleSuspendedTransactions", rbNull, INITIATIVE_ID);
        Mono<RewardBatch> r2 = ReflectionTestUtils.invokeMethod(serviceSpy, "handleSuspendedTransactions", rbZero, INITIATIVE_ID);

        StepVerifier.create(r1).expectNext(rbNull).verifyComplete();
        StepVerifier.create(r2).expectNext(rbZero).verifyComplete();
    }

    @Test
    void updateNewBatchCounters_handlesNullsAndAdds() {
        RewardBatch newBatch = RewardBatch.builder().id(BATCH_ID_2).build();
        service.updateNewBatchCounters(newBatch, 500L, 3L);

        assertEquals(500L, newBatch.getInitialAmountCents());
        assertEquals(3L, newBatch.getNumberOfTransactionsSuspended());
        assertEquals(3L, newBatch.getNumberOfTransactions());
        assertEquals(500L, newBatch.getSuspendedAmountCents());

        service.updateNewBatchCounters(newBatch, 200L, 2L);
        assertEquals(700L, newBatch.getInitialAmountCents());
        assertEquals(5L, newBatch.getNumberOfTransactionsSuspended());
        assertEquals(5L, newBatch.getNumberOfTransactions());
        assertEquals(200L, newBatch.getSuspendedAmountCents());
    }

    @Test
    void createRewardBatchAndSave_existingBatchFound() {
        RewardBatch savedBatch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .month("2025-12")
                .name("dicembre 2025")
                .posType(PHYSICAL)
                .status(RewardBatchStatus.APPROVED)
                .partial(false)
                .build();

        RewardBatch existing = RewardBatch.builder().id(BATCH_ID_2).merchantId(MERCHANT_ID).month("2026-01").name("gennaio 2026").posType(PHYSICAL).build();

        when(rewardBatchRepository.findRewardBatchByFilter(null, MERCHANT_ID, PHYSICAL, "2026-01"))
                .thenReturn(Mono.just(existing));

        StepVerifier.create(service.createRewardBatchAndSave(savedBatch))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(argThat(b -> b.getId() == null));
    }

    @Test
    void createRewardBatchAndSave_createsWhenMissing() {
        RewardBatch savedBatch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .month("2025-12")
                .name("dicembre 2025")
                .posType(PHYSICAL)
                .partial(false)
                .build();

        when(rewardBatchRepository.findRewardBatchByFilter(null, MERCHANT_ID, PHYSICAL, "2026-01"))
                .thenReturn(Mono.empty());

        when(rewardBatchRepository.save(any()))
                .thenAnswer(inv -> {
                    RewardBatch b = inv.getArgument(0);
                    b.setId(BATCH_ID_2);
                    return Mono.just(b);
                });

        StepVerifier.create(service.createRewardBatchAndSave(savedBatch))
                .assertNext(b -> {
                    assertEquals(BATCH_ID_2, b.getId());
                    assertEquals("2026-01", b.getMonth());
                    assertEquals(RewardBatchStatus.CREATED, b.getStatus());
                    assertEquals(RewardBatchAssignee.L1, b.getAssigneeLevel());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(any());
    }

    @Test
    void addOneMonth_and_italian() {
        assertEquals("2026-01", service.addOneMonth("2025-12"));
        assertEquals("gennaio 2026", service.addOneMonthToItalian("dicembre 2025"));
    }

    @Test
    void updateAndSaveRewardTransactionsToApprove_setsApprovedAndSaves() {
        RewardTransaction t1 = new RewardTransaction();
        t1.setId("T1");
        t1.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);

        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList()))
                .thenReturn(Flux.just(t1));
        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.updateAndSaveRewardTransactionsToApprove(BATCH_ID, INITIATIVE_ID))
                .verifyComplete();

        verify(rewardTransactionRepository).save(argThat(t -> t.getRewardBatchTrxStatus() == RewardBatchTrxStatus.APPROVED));
    }

    @Test
    void updateAndSaveRewardTransactionsSuspended_empty_returnsZero() {
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList()))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.updateAndSaveRewardTransactionsSuspended(BATCH_ID, INITIATIVE_ID, BATCH_ID_2, "2025-12"))
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void updateAndSaveRewardTransactionsSuspended_movesAndSums_andSetsLastMonthIfNull() {
        RewardTransaction t1 = RewardTransaction.builder()
                .id("T1")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(BATCH_ID)
                .rewardBatchLastMonthElaborated(null)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardTransaction t2 = RewardTransaction.builder()
                .id("T2")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchId(BATCH_ID)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(null)
                .build();

        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList()))
                .thenReturn(Flux.just(t1, t2));
        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.updateAndSaveRewardTransactionsSuspended(BATCH_ID, INITIATIVE_ID, BATCH_ID_2, "2025-12"))
                .expectNext(100L)
                .verifyComplete();

        assertEquals(BATCH_ID_2, t1.getRewardBatchId());
        assertEquals("2025-12", t1.getRewardBatchLastMonthElaborated());
        assertEquals(BATCH_ID_2, t2.getRewardBatchId());
        assertEquals("2025-11", t2.getRewardBatchLastMonthElaborated());
    }

    @Test
    void validateRewardBatch_notFound() {
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchNotFound.class)
                .verify();
    }

    @Test
    void validateRewardBatch_L1_to_L2_success() {
        RewardBatch b = RewardBatch.builder()
                .id(BATCH_ID)
                .assigneeLevel(RewardBatchAssignee.L1)
                .numberOfTransactions(100L)
                .numberOfTransactionsElaborated(20L)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> assertEquals(RewardBatchAssignee.L2, updated.getAssigneeLevel()))
                .verifyComplete();
    }

    @Test
    void validateRewardBatch_L1_wrongRole() {
        RewardBatch b = RewardBatch.builder()
                .id(BATCH_ID)
                .assigneeLevel(RewardBatchAssignee.L1)
                .numberOfTransactions(100L)
                .numberOfTransactionsElaborated(20L)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.validateRewardBatch("guest", INITIATIVE_ID, BATCH_ID))
                .expectError(RoleNotAllowedForL1PromotionException.class)
                .verify();
    }

    @Test
    void validateRewardBatch_L1_not15percent() {
        RewardBatch b = RewardBatch.builder()
                .id(BATCH_ID)
                .assigneeLevel(RewardBatchAssignee.L1)
                .numberOfTransactions(100L)
                .numberOfTransactionsElaborated(10L)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(BatchNotElaborated15PercentException.class)
                .verify();
    }

    @Test
    void validateRewardBatch_L2_to_L3_success() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.validateRewardBatch(OP2, INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> assertEquals(RewardBatchAssignee.L3, updated.getAssigneeLevel()))
                .verifyComplete();
    }

    @Test
    void validateRewardBatch_L2_wrongRole() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.validateRewardBatch("guest", INITIATIVE_ID, BATCH_ID))
                .expectError(RoleNotAllowedForL2PromotionException.class)
                .verify();
    }

    @Test
    void validateRewardBatch_invalidState() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L3).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.validateRewardBatch(OP3, INITIATIVE_ID, BATCH_ID))
                .expectError(InvalidBatchStateForPromotionException.class)
                .verify();
    }

    @Test
    void generateAndSaveCsv_invalidBatchId_fastFail() {
        StepVerifier.create(service.generateAndSaveCsv("bad/..", INITIATIVE_ID, MERCHANT_ID))
                .expectError(IllegalArgumentException.class)
                .verify();

        verify(rewardBatchRepository, never()).findById(anyString());
    }

    @Test
    void generateAndSaveCsv_success_withCFandWithoutCF() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName("Biz")
                .name("dicembre 2025")
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        RewardTransaction trxWithCF = RewardTransaction.builder()
                .id("T1")
                .trxChargeDate(LocalDateTime.of(2025, 12, 10, 10, 30))
                .fiscalCode("CF1")
                .trxCode("CODE")
                .effectiveAmountCents(1000L)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .additionalProperties(Map.of("productName", "Lavatrice", "productGtin", "803"))
                .invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().docNumber("DOC").filename("inv.pdf").build())
                .franchiseName("Store")
                .build();

        RewardTransaction trxNoCF = RewardTransaction.builder()
                .id("T2")
                .userId("U2")
                .fiscalCode(null)
                .trxCode("CODE2")
                .effectiveAmountCents(2000L)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build()))
                .additionalProperties(Map.of("productName", "Aspirapolvere", "productGtin", "123"))
                .invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().docNumber(null).filename("inv2.pdf").build())
                .franchiseName("Store2")
                .build();

        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList()))
                .thenReturn(Flux.just(trxWithCF, trxNoCF));

        when(userRestClient.retrieveUserInfo("U2"))
                .thenReturn(Mono.just(it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV.builder().pii("CF2").build()));

        doReturn(Mono.just("some/path/file.csv"))
                .when(serviceSpy).uploadCsvToBlob(anyString(), anyString());

        StepVerifier.create(serviceSpy.generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID))
                .assertNext(filename -> assertTrue(filename.endsWith(".csv")))
                .verifyComplete();

        verify(rewardBatchRepository).save(argThat(b -> b.getFilename() != null && b.getFilename().endsWith(".csv")));
        assertEquals("CF2", trxNoCF.getFiscalCode());
    }

    @Test
    void generateAndSaveCsv_uploadFails_propagates() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName("Biz")
                .name("dicembre 2025")
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));

        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .fiscalCode("CF")
                .trxCode("CODE")
                .effectiveAmountCents(1000L)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .additionalProperties(Map.of())
                .invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().filename("inv.pdf").build())
                .franchiseName("Store")
                .build();

        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList()))
                .thenReturn(Flux.just(trx));

        doReturn(Mono.error(new RuntimeException("upload fail")))
                .when(serviceSpy).uploadCsvToBlob(anyString(), anyString());

        StepVerifier.create(serviceSpy.generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID))
                .expectError(RuntimeException.class)
                .verify();

        verify(rewardBatchRepository, never()).save(any());
    }

    @Test
    void csvField_private_behavior() {
        String r1 = ReflectionTestUtils.invokeMethod(service, "csvField", (String) null);
        assertEquals("", r1);

        String r2 = ReflectionTestUtils.invokeMethod(service, "csvField", "plain");
        assertEquals("plain", r2);

        String r3 = ReflectionTestUtils.invokeMethod(service, "csvField", "a;b");
        assertEquals("\"a;b\"", r3);

        String r4 = ReflectionTestUtils.invokeMethod(service, "csvField", "a\"b");
        assertEquals("\"a\"\"b\"", r4);
    }

    @Test
    void mapTransactionToCsvRow_private_handlesNullsAndFormatting() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .trxChargeDate(null)
                .fiscalCode("CF")
                .trxCode("CODE")
                .effectiveAmountCents(null)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(null).build()))
                .additionalProperties(Map.of("productName", "Prod;X", "productGtin", "GTIN"))
                .invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().docNumber(null).filename("inv.pdf").build())
                .franchiseName("Store")
                .build();

        String row = ReflectionTestUtils.invokeMethod(service, "mapTransactionToCsvRow", trx, INITIATIVE_ID);
        assertNotNull(row);
        assertTrue(row.contains("T1"));
        assertTrue(row.contains("\"Prod;X"));
    }

    @Test
    void uploadCsvToBlob_success_status201() {
        @SuppressWarnings("unchecked")
        Response<BlockBlobItem> resp = Mockito.mock(Response.class);
        when(resp.getStatusCode()).thenReturn(HttpStatus.CREATED.value());

        when(approvedRewardBatchBlobService.upload(any(), anyString(), anyString()))
                .thenReturn(resp);

        StepVerifier.create(service.uploadCsvToBlob("file.csv", "content"))
                .expectNext("file.csv")
                .verifyComplete();
    }

    @Test
    void uploadCsvToBlob_statusNot201_throwsClientExceptionWithBody() {
        @SuppressWarnings("unchecked")
        Response<BlockBlobItem> resp = Mockito.mock(Response.class);
        when(resp.getStatusCode()).thenReturn(HttpStatus.BAD_REQUEST.value());

        when(approvedRewardBatchBlobService.upload(any(), anyString(), anyString()))
                .thenReturn(resp);

        StepVerifier.create(service.uploadCsvToBlob("file.csv", "content"))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(ClientExceptionWithBody.class, ex);
                    ClientExceptionWithBody ce = (ClientExceptionWithBody) ex;
                    assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, ce.getCode());
                })
                .verify();
    }


    @Test
    void postponeTransaction_transactionNotFound() {
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.now()))
                .expectError(ClientExceptionNoBody.class)
                .verify();
    }

    @Test
    void postponeTransaction_batchNotFound() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.now()))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void postponeTransaction_invalidBatchStatus() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2025-12")
                .status(RewardBatchStatus.SENT)
                .build();

        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.now()))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void postponeTransaction_limitExceeded() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-12")
                .status(RewardBatchStatus.CREATED)
                .build();

        LocalDate initiativeEnd = LocalDate.of(2026, 1, 6);

        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", initiativeEnd))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void postponeTransaction_nextBatchNotCreatedStatus_throwsNoBody() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-01")
                .status(RewardBatchStatus.CREATED)
                .build();

        RewardBatch next = RewardBatch.builder()
                .id(BATCH_ID_2)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-02")
                .status(RewardBatchStatus.APPROVED)
                .build();

        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));

        doReturn(Mono.just(next)).when(serviceSpy).findOrCreateBatch(MERCHANT_ID, PHYSICAL, "2026-02", BUSINESS_NAME);

        StepVerifier.create(serviceSpy.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.of(2026, 1, 6)))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verify(serviceSpy, never()).decrementTotals(anyString(), anyLong());
        verify(serviceSpy, never()).incrementTotals(anyString(), anyLong());
        verify(rewardTransactionRepository, never()).save(any());
    }

    @Test
    void postponeTransaction_success_movesAndUpdatesTrx() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-01")
                .status(RewardBatchStatus.CREATED)
                .build();

        RewardBatch next = RewardBatch.builder()
                .id(BATCH_ID_2)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-02")
                .status(RewardBatchStatus.CREATED)
                .build();

        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));

        doReturn(Mono.just(next)).when(serviceSpy).findOrCreateBatch(MERCHANT_ID, PHYSICAL, "2026-02", BUSINESS_NAME);
        doReturn(Mono.just(current)).when(serviceSpy).decrementTotals(BATCH_ID, 100L);
        doReturn(Mono.just(next)).when(serviceSpy).incrementTotals(BATCH_ID_2, 100L);

        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(serviceSpy.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.of(2026, 1, 6)))
                .verifyComplete();

        assertEquals(BATCH_ID_2, trx.getRewardBatchId());
        assertNotNull(trx.getRewardBatchInclusionDate());
        assertNotNull(trx.getUpdateDate());
    }

    @Test
    void deleteEmptyRewardBatches_deletesMatching() {
        RewardBatch b1 = RewardBatch.builder().id("D1").month("2025-10").numberOfTransactions(0L).build();
        RewardBatch b2 = RewardBatch.builder().id("D2").month("2025-09").numberOfTransactions(0L).build();

        com.mongodb.reactivestreams.client.MongoDatabase db = mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        when(db.getName()).thenReturn("db");

        when(reactiveMongoTemplate.getMongoDatabase()).thenReturn(Mono.just(db));
        when(reactiveMongoTemplate.getCollectionName(RewardBatch.class)).thenReturn("rewardBatch");

        when(reactiveMongoTemplate.count(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Mono.just(10L))
                .thenReturn(Mono.just(2L));

        when(reactiveMongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Flux.just(b1, b2));

        when(reactiveMongoTemplate.remove(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Mono.just(DeleteResult.acknowledged(1L)));

        StepVerifier.create(service.deleteEmptyRewardBatches())
                .verifyComplete();

        verify(reactiveMongoTemplate, times(2)).remove(any(Query.class), eq(RewardBatch.class));
    }

    @Test
    void deleteEmptyRewardBatches_noMatches() {
        com.mongodb.reactivestreams.client.MongoDatabase db = mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        when(db.getName()).thenReturn("db");

        when(reactiveMongoTemplate.getMongoDatabase()).thenReturn(Mono.just(db));
        when(reactiveMongoTemplate.getCollectionName(RewardBatch.class)).thenReturn("rewardBatch");

        when(reactiveMongoTemplate.count(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Mono.just(10L))
                .thenReturn(Mono.just(0L));

        when(reactiveMongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.deleteEmptyRewardBatches())
                .verifyComplete();

        verify(reactiveMongoTemplate, never()).remove(any(Query.class), eq(RewardBatch.class));
    }
}
