package it.gov.pagopa.idpay.transactions.service;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;
import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErrorInvitaliaDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchListPort;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoMerchantRewardBatchLookupAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoRewardBatchAssigneePromotionAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoRewardBatchDeliveryAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoRewardBatchFinalApprovalAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoRewardBatchLifecycleAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoRewardTransactionAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoRewardBatchTransactionMutationAdapter;
import it.gov.pagopa.idpay.transactions.persistence.mongo.MongoSuspendedTransactionReassignmentAdapter;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchAssigneePromotionPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchDeliveryPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchFinalApprovalPort;
import it.gov.pagopa.idpay.transactions.persistence.port.SuspendedTransactionReassignmentPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.YearMonth;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_CHECKS_ERROR;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.MERCHANT_OR_OPERATOR_HEADER_MANDATORY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardBatchListPort rewardBatchListPort;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private UserRestClient userRestClient;
    @Mock private ApprovedRewardBatchBlobService approvedRewardBatchBlobService;
    @Mock private ChecksErrorMapper checksErrorMapper;
    @Mock private AuditUtilities auditUtilities;

    @Mock private MerchantRestClient  merchantRestClient;
    @Mock private SelfcareInstitutionsRestClient  selfcareInstitutionsRestClient;
    @Mock private ErogazioniRestClient  erogazioniRestClient;
    @Mock private InitiativeDataService initiativeDataService;
    @Mock private RewardBatchFinalApprovalPort rewardBatchFinalApprovalPort;
    @Mock private RewardBatchAssigneePromotionPort rewardBatchAssigneePromotionPort;
    @Mock private RewardBatchDeliveryPort rewardBatchDeliveryPort;
    @Mock private SuspendedTransactionReassignmentPort suspendedTransactionReassignmentPort;

    private RewardBatchServiceImpl service;
    private RewardBatchServiceImpl serviceSpy;

    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final List<String> INITIATIVES_ID = List.of(INITIATIVE_ID);
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
                new MongoRewardBatchLifecycleAdapter(rewardBatchRepository),
                rewardBatchListPort,
                new MongoMerchantRewardBatchLookupAdapter(rewardBatchRepository),
                rewardTransactionRepository,
                new MongoRewardTransactionAdapter(rewardTransactionRepository, rewardBatchRepository),
                new MongoRewardTransactionAdapter(rewardTransactionRepository, rewardBatchRepository),
                new MongoRewardBatchTransactionMutationAdapter(
                        rewardTransactionRepository,
                        rewardBatchRepository
                ),
                new MongoRewardBatchFinalApprovalAdapter(rewardBatchRepository, rewardTransactionRepository),
                new MongoRewardBatchAssigneePromotionAdapter(rewardBatchRepository, rewardTransactionRepository),
                new MongoRewardBatchDeliveryAdapter(rewardBatchRepository),
                new MongoSuspendedTransactionReassignmentAdapter(
                        rewardBatchRepository,
                        new MongoRewardBatchTransactionMutationAdapter(
                                rewardTransactionRepository,
                                rewardBatchRepository
                        )
                ),
                userRestClient,
                approvedRewardBatchBlobService,
                checksErrorMapper,
                auditUtilities,
                merchantRestClient,
                selfcareInstitutionsRestClient,
                erogazioniRestClient,
                initiativeDataService,
                10

        );
        serviceSpy = spy(service);
        lenient().when(rewardTransactionRepository.findByRewardBatchIdAndInitiativeId(anyString(), anyString()))
                .thenReturn(Flux.empty());
    }

    @Test
    void workerPorts_shouldDelegateApprovalAndPromotion() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch approving = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L3).build();
        RewardBatch l1 = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L1).build();
        RewardBatch l2 = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();

        when(rewardBatchFinalApprovalPort.prepareFinalApproval(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(approving));
        when(rewardBatchAssigneePromotionPort.findBatchForPromotion(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(l1));
        when(rewardBatchAssigneePromotionPort.promote(
                BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L1, RewardBatchAssignee.L2
        )).thenReturn(Mono.just(l2));
        StepVerifier.create(delegatedService.updateAndSaveRewardTransactionsToApprove(BATCH_ID, INITIATIVE_ID))
                .verifyComplete();
        StepVerifier.create(delegatedService.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .expectNext(l2)
                .verifyComplete();

        verify(rewardBatchFinalApprovalPort).prepareFinalApproval(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchAssigneePromotionPort).promote(
                BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L1, RewardBatchAssignee.L2
        );
    }

    @Test
    void confirmationWorker_shouldDelegateSuspendedReassignmentBeforeCompletingApproval() {
        RewardBatchServiceImpl delegatedService = spy(serviceWithWorkerPorts());
        RewardBatch beforeApproval = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).numberOfTransactionsSuspended(0L).build();
        RewardBatch prepared = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).numberOfTransactionsSuspended(1L).build();
        RewardBatch approved = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(beforeApproval));
        when(rewardBatchFinalApprovalPort.prepareFinalApproval(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(prepared));
        when(suspendedTransactionReassignmentPort.reassignSuspendedTransactions(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.empty());
        when(rewardBatchFinalApprovalPort.completeFinalApproval(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(approved));
        doReturn(Mono.just("approved.csv")).when(delegatedService)
                .generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID);

        StepVerifier.create(delegatedService.processSingleBatchConfirmation(beforeApproval, INITIATIVE_ID))
                .expectNext(approved)
                .verifyComplete();

        verify(suspendedTransactionReassignmentPort).reassignSuspendedTransactions(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchFinalApprovalPort).completeFinalApproval(BATCH_ID, INITIATIVE_ID);
    }

    @Test
    void deliveryWorker_shouldUseSnapshottedAmountAndDelegateOutcome() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch requested = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED).build();
        RewardBatch snapshotted = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED)
                .approvedAmountCents(999L).deliveryAmountCents(123L).approvalDate(LocalDateTime.of(2026, Month.JULY, 1, 9, 0))
                .build();
        DeliveryOutcomeDTO outcome = DeliveryOutcomeDTO.builder().succeded(true).message("accepted").build();
        RewardBatch delivered = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        MerchantDetailDTO merchant = MerchantDetailDTO.builder().fiscalCode("fiscal").vatNumber("vat")
                .businessName(BUSINESS_NAME).iban("iban").ibanHolder("holder").build();
        InstitutionDTO institution = InstitutionDTO.builder().zipCode("00100").build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(requested));
        when(rewardBatchDeliveryPort.snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(snapshotted));
        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID)).thenReturn(Mono.just(merchant));
        when(selfcareInstitutionsRestClient.getInstitutions("fiscal"))
                .thenReturn(Mono.just(new InstitutionList(List.of(institution))));
        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class))).thenReturn(Mono.just(outcome));
        when(rewardBatchDeliveryPort.recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome)).thenReturn(Mono.just(delivered));

        StepVerifier.create(delegatedService.processSingleBatchDelivery(requested, INITIATIVE_ID))
                .expectNext(delivered)
                .verifyComplete();

        org.mockito.ArgumentCaptor<DeliveryRequest> requestCaptor =
                org.mockito.ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(erogazioniRestClient).postErogazione(requestCaptor.capture());
        assertEquals(1.23, requestCaptor.getValue().getErogazione().getImporto());
        verify(rewardBatchDeliveryPort).recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome);
    }

    @Test
    void deliveryWorker_shouldSurfaceOutcomePersistenceStateMismatch() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch requested = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED).build();
        RewardBatch snapshotted = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED)
                .deliveryAmountCents(123L).approvalDate(LocalDateTime.of(2026, Month.JULY, 1, 9, 0))
                .build();
        DeliveryOutcomeDTO outcome = DeliveryOutcomeDTO.builder().succeded(true).message("accepted").build();
        MerchantDetailDTO merchant = MerchantDetailDTO.builder().fiscalCode("fiscal").vatNumber("vat")
                .businessName(BUSINESS_NAME).iban("iban").ibanHolder("holder").build();
        InstitutionDTO institution = InstitutionDTO.builder().zipCode("00100").build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(requested));
        when(rewardBatchDeliveryPort.snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(snapshotted));
        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID)).thenReturn(Mono.just(merchant));
        when(selfcareInstitutionsRestClient.getInstitutions("fiscal"))
                .thenReturn(Mono.just(new InstitutionList(List.of(institution))));
        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class))).thenReturn(Mono.just(outcome));
        when(rewardBatchDeliveryPort.recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome)).thenReturn(Mono.empty());

        StepVerifier.create(delegatedService.processSingleBatchDelivery(requested, INITIATIVE_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verify(rewardBatchDeliveryPort).recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome);
    }

    @Test
    void deliveryWorker_shouldReturnRejectedDeliveryWithoutOutcomeWithoutMovingBatchToPendingRefund() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch requested = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED).build();
        RewardBatch snapshotted = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED)
                .deliveryAmountCents(123L).approvalDate(LocalDateTime.of(2026, Month.JULY, 1, 9, 0))
                .build();
        DeliveryOutcomeDTO outcome = DeliveryOutcomeDTO.builder().succeded(false).message("rejected").build();
        RewardBatch rejectedWithOutcome = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED)
                .deliveryOutcome(outcome).build();
        RewardBatch rejected = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).build();
        MerchantDetailDTO merchant = MerchantDetailDTO.builder().fiscalCode("fiscal").vatNumber("vat")
                .businessName(BUSINESS_NAME).iban("iban").ibanHolder("holder").build();
        InstitutionDTO institution = InstitutionDTO.builder().zipCode("00100").build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(requested), Mono.just(requested));
        when(rewardBatchDeliveryPort.snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(snapshotted), Mono.just(snapshotted));
        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID)).thenReturn(Mono.just(merchant));
        when(selfcareInstitutionsRestClient.getInstitutions("fiscal"))
                .thenReturn(Mono.just(new InstitutionList(List.of(institution))));
        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class))).thenReturn(Mono.just(outcome));
        when(rewardBatchDeliveryPort.recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome))
                .thenReturn(Mono.just(rejectedWithOutcome), Mono.just(rejected));

        StepVerifier.create(delegatedService.processSingleBatchDelivery(requested, INITIATIVE_ID))
                .assertNext(result -> {
                    assertSame(rejectedWithOutcome, result);
                    assertEquals(RewardBatchStatus.APPROVED, result.getStatus());
                    assertEquals(outcome, result.getDeliveryOutcome());
                })
                .verifyComplete();

        StepVerifier.create(delegatedService.processSingleBatchDelivery(requested, INITIATIVE_ID))
                .assertNext(result -> {
                    assertSame(rejected, result);
                    assertEquals(RewardBatchStatus.APPROVED, result.getStatus());
                    assertNull(result.getDeliveryOutcome());
                })
                .verifyComplete();
    }

    @Test
    void updateBatch_shouldDelegateOnlyTerminalRefundOutcomes() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch pending = RewardBatch.builder().id(BATCH_ID).initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.PENDING_REFUND).build();
        RewardBatch refunded = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.REFUNDED).build();
        InvitaliaOutcomeResponseDTO completed = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(ErogazioneOutcomeDTO.builder().status("COMPLETATA").dateValue(LocalDate.of(2026, Month.JULY, 2)).build())
                .build();
        when(rewardBatchDeliveryPort.recordRefundOutcome(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.REFUNDED, LocalDate.of(2026, Month.JULY, 2), null
        )).thenReturn(Mono.just(refunded));

        StepVerifier.create(delegatedService.updateBatch(pending, completed))
                .expectNext(refunded)
                .verifyComplete();
        verify(rewardBatchDeliveryPort).recordRefundOutcome(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.REFUNDED, LocalDate.of(2026, Month.JULY, 2), null
        );
    }

    @Test
    void updateBatch_shouldSurfaceARefundOutcomeStateMismatch() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch pending = RewardBatch.builder().id(BATCH_ID).initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO completed = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(ErogazioneOutcomeDTO.builder().status("COMPLETATA").dateValue(LocalDate.of(2026, Month.JULY, 2)).build())
                .build();
        when(rewardBatchDeliveryPort.recordRefundOutcome(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.REFUNDED, LocalDate.of(2026, Month.JULY, 2), null
        )).thenReturn(Mono.empty());

        StepVerifier.create(delegatedService.updateBatch(pending, completed))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void updateBatch_shouldLeaveUnknownExternalOutcomeUnchanged() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch pending = RewardBatch.builder().id(BATCH_ID).initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO unknown = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(ErogazioneOutcomeDTO.builder().status("UNKNOWN").build())
                .build();

        StepVerifier.create(delegatedService.updateBatch(pending, unknown))
                .expectNext(pending)
                .verifyComplete();

        verifyNoInteractions(rewardBatchDeliveryPort);
    }

    @Test
    void updateBatch_shouldPersistRefundRejectionWithExternalErrors() {
        RewardBatchServiceImpl delegatedService = serviceWithWorkerPorts();
        RewardBatch pending = RewardBatch.builder().id(BATCH_ID).initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO rejected = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(ErogazioneOutcomeDTO.builder().status("RIFIUTATA").build())
                .errors(List.of(
                        new ErrorInvitaliaDTO("first", "first error"),
                        new ErrorInvitaliaDTO("second", "second error")
                ))
                .build();
        RewardBatch notRefunded = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.NOT_REFUNDED).build();
        when(rewardBatchDeliveryPort.recordRefundOutcome(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.NOT_REFUNDED, null,
                "first - first error; second - second error"
        )).thenReturn(Mono.just(notRefunded));

        StepVerifier.create(delegatedService.updateBatch(pending, rejected))
                .expectNext(notRefunded)
                .verifyComplete();

        verify(rewardBatchDeliveryPort).recordRefundOutcome(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.NOT_REFUNDED, null,
                "first - first error; second - second error"
        );
    }

    @Test
    void confirmationWorker_shouldNotReassignWhenPreparationFindsNoSuspendedTransactions() {
        RewardBatchServiceImpl delegatedService = spy(serviceWithWorkerPorts());
        RewardBatch beforeApproval = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).build();
        RewardBatch prepared = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).numberOfTransactionsSuspended(0L).build();
        RewardBatch approved = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(beforeApproval));
        when(rewardBatchFinalApprovalPort.prepareFinalApproval(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(prepared));
        when(rewardBatchFinalApprovalPort.completeFinalApproval(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(approved));
        doReturn(Mono.just("approved.csv")).when(delegatedService)
                .generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID);

        StepVerifier.create(delegatedService.processSingleBatchConfirmation(beforeApproval, INITIATIVE_ID))
                .expectNext(approved)
                .verifyComplete();

        verify(suspendedTransactionReassignmentPort, never())
                .reassignSuspendedTransactions(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchFinalApprovalPort).completeFinalApproval(BATCH_ID, INITIATIVE_ID);
    }

    @Test
    void confirmationWorker_shouldNotReassignWhenPreparationDoesNotReportSuspendedTransactions() {
        RewardBatchServiceImpl delegatedService = spy(serviceWithWorkerPorts());
        RewardBatch beforeApproval = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).build();
        RewardBatch prepared = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).numberOfTransactionsSuspended(null).build();
        RewardBatch approved = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).status(RewardBatchStatus.APPROVED).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(beforeApproval));
        when(rewardBatchFinalApprovalPort.prepareFinalApproval(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(prepared));
        when(rewardBatchFinalApprovalPort.completeFinalApproval(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(approved));
        doReturn(Mono.just("approved.csv")).when(delegatedService)
                .generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID);

        StepVerifier.create(delegatedService.processSingleBatchConfirmation(beforeApproval, INITIATIVE_ID))
                .expectNext(approved)
                .verifyComplete();

        verify(suspendedTransactionReassignmentPort, never())
                .reassignSuspendedTransactions(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchFinalApprovalPort).completeFinalApproval(BATCH_ID, INITIATIVE_ID);
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

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(INITIATIVE_ID, "M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.just(existing));

        StepVerifier.create(service.findOrCreateBatch(INITIATIVE_ID, "M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(any());
    }

    @Test
    void findOrCreateBatch_createsNew_whenMissing() {
        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(INITIATIVE_ID, "M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.empty());

        when(rewardBatchRepository.save(any()))
                .thenAnswer(inv -> {
                    RewardBatch b = inv.getArgument(0);
                    b.setId("NEW");
                    return Mono.just(b);
                });

        StepVerifier.create(service.findOrCreateBatch(INITIATIVE_ID, "M1", PHYSICAL, "2025-11", BUSINESS_NAME))
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

        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(INITIATIVE_ID, "M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.empty())
                .thenReturn(Mono.just(existing));

        when(rewardBatchRepository.save(any()))
                .thenReturn(Mono.error(new DuplicateKeyException("dup")));

        StepVerifier.create(service.findOrCreateBatch(INITIATIVE_ID, "M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository).save(any());
        verify(rewardBatchRepository, times(2)).findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(INITIATIVE_ID, "M1", PHYSICAL, "2025-11");
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

        when(rewardBatchListPort.findRewardBatches(null, null, null, null, null, true, pageable))
                .thenReturn(Flux.just(b1, b2));
        when(rewardBatchListPort.countRewardBatches(null, null, null, null, null, true))
                .thenReturn(Mono.just(10L));

        StepVerifier.create(service.getRewardBatches(null, null, "operator1", null, null, null, pageable))
                .assertNext(p -> {
                    assertEquals(2, p.getContent().size());
                    assertEquals(10L, p.getTotalElements());
                })
                .verifyComplete();

        when(rewardBatchListPort.findRewardBatches("M1", INITIATIVE_ID, null, null, null, false, pageable))
                .thenReturn(Flux.just(b1));
        when(rewardBatchListPort.countRewardBatches("M1", INITIATIVE_ID, null, null, null, false))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getRewardBatches("M1", INITIATIVE_ID, "guest", null, null, null, pageable))
                .assertNext(p -> {
                    assertEquals(1, p.getContent().size());
                    assertEquals(1L, p.getTotalElements());
                })
                .verifyComplete();
    }


    @Test
    void sendRewardBatch_batchNotFound() {
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_merchantMismatch() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId("OTHER").status(RewardBatchStatus.CREATED).month("2025-01").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_invalidStatus() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.SENT).month("2025-01").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();
    }

    @Test
    void sendRewardBatch_monthTooEarly() {
        YearMonth now = YearMonth.now();
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.CREATED).month(now.toString()).posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchException.class, ex);
                    assertTrue(ex.getMessage().contains("REWARD_BATCH_MONTH_TOO_EARLY"));
                })
                .verify();
    }

    @Test
    void sendRewardBatch_previousNotSentNotEmpty() {
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
                .numberOfTransactions(1L)
                .month(batchMonth.minusMonths(1).toString())
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndPosType(MERCHANT_ID, INITIATIVE_ID, PHYSICAL))
                .thenReturn(Flux.just(previousCreated));

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class)
                .verify();

    }

    @Test
    void sendRewardBatch_emptyCreatedAfterMonth_persistsSentWhenChronologyAllows() {
        YearMonth batchMonth = YearMonth.now().minusMonths(1);

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.CREATED)
                .numberOfTransactions(0L)
                .month(batchMonth.toString())
                .posType(PHYSICAL)
                .build();

        RewardBatch previousCreatedEmpty = RewardBatch.builder()
                .id("PREV")
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.CREATED)
                .numberOfTransactions(0L)
                .month(batchMonth.minusMonths(1).toString())
                .posType(PHYSICAL)
                .build();


        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndPosType(MERCHANT_ID, INITIATIVE_ID, PHYSICAL))
                .thenReturn(Flux.just(previousCreatedEmpty));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
                .verifyComplete();

        verify(rewardBatchRepository).save(argThat(b -> b.getStatus() == RewardBatchStatus.SENT && b.getMerchantSendDate() != null));

    }

    @Test
    void sendRewardBatch_success_allPreviousSent() {
        YearMonth batchMonth = YearMonth.now().minusMonths(1);

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.CREATED)
                .month(batchMonth.toString())
                .posType(PHYSICAL)
                .build();

        RewardBatch previousSent = RewardBatch.builder()
                .id("PREV")
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.SENT)
                .month(batchMonth.minusMonths(1).toString())
                .posType(PHYSICAL)
                .build();

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndPosType(MERCHANT_ID, INITIATIVE_ID, PHYSICAL))
                .thenReturn(Flux.just(previousSent));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.sendRewardBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID))
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
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.suspendTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(), any(), any(), any(), any());
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

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction trxSuspPrev = RewardTransaction.builder()
                .id("SUSP_PREV")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardTransaction trxApproved = RewardTransaction.builder()
                .id("APP")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(200L).build()))
                .build();

        RewardTransaction trxToCheck = RewardTransaction.builder()
                .id("TO_CHECK")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(300L).build()))
                .build();

        RewardTransaction trxConsultable = RewardTransaction.builder()
                .id("CONS")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(400L).build()))
                .build();

        RewardTransaction trxRejected = RewardTransaction.builder()
                .id("REJ")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(500L).build()))
                .build();

        RewardTransaction trxNullAccrued = RewardTransaction.builder()
                .id("NULL_ACC")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of("OTHER", Reward.builder().accruedRewardCents(999L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("SUSP_PREV"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model)))
                .thenReturn(Mono.just(trxSuspPrev));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("APP"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq( model)))
                .thenReturn(Mono.just(trxApproved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("TO_CHECK"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model)))
                .thenReturn(Mono.just(trxToCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("CONS"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model)))
                .thenReturn(Mono.just(trxConsultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("REJ"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model)))
                .thenReturn(Mono.just(trxRejected));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("NULL_ACC"), eq( RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model)))
                .thenReturn(Mono.just(trxNullAccrued));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(updated));

        StepVerifier.create(service.suspendTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectNext(updated)
                .verifyComplete();

        verify(auditUtilities).logTransactionsStatusChanged(eq(RewardBatchTrxStatus.SUSPENDED.name()), eq(INITIATIVE_ID), anyString(), eq(checks));
        verify(rewardBatchRepository).updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any(BatchCountersDTO.class));
    }

    @Test
    void suspendTransactions_alreadySuspended_sameMonth_skipsElaboratedIncrement() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.EVALUATING)
                .month(batchMonth)
                .build();

        ChecksErrorDTO checks = new ChecksErrorDTO();
        checks.setCfError(true);

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("SUSP_SAME"))
                .reason("R")
                .checksError(checks)
                .build();

        ChecksError model = new ChecksError();
        when(checksErrorMapper.toModel(checks)).thenReturn(model);
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction trxSuspSame = RewardTransaction.builder()
                .id("SUSP_SAME")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-12")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("SUSP_SAME"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model)))
                .thenReturn(Mono.just(trxSuspSame));

        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any(BatchCountersDTO.class)))
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

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction alreadyRejected = RewardTransaction.builder()
                .id("ALREADY_REJ")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(10L).build()))
                .build();

        RewardTransaction approved = RewardTransaction.builder()
                .id("APP")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(20L).build()))
                .build();

        RewardTransaction toCheck = RewardTransaction.builder()
                .id("TO_CHECK")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(30L).build()))
                .build();

        RewardTransaction consultable = RewardTransaction.builder()
                .id("CONS")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(40L).build()))
                .build();

        RewardTransaction suspendedPrev = RewardTransaction.builder()
                .id("SUSP_PREV")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build()))
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("ALREADY_REJ"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null)))
                .thenReturn(Mono.just(alreadyRejected));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("APP"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null)))
                .thenReturn(Mono.just(approved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("TO_CHECK"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null)))
                .thenReturn(Mono.just(toCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("CONS"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null)))
                .thenReturn(Mono.just(consultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(INITIATIVE_ID), eq(BATCH_ID), eq("SUSP_PREV"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null)))
                .thenReturn(Mono.just(suspendedPrev));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(updated));

        StepVerifier.create(service.rejectTransactions(BATCH_ID, INITIATIVE_ID, req))
                .expectNext(updated)
                .verifyComplete();
    }

    @Test
    void approvedTransactions_allBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.EVALUATING)
                .month(batchMonth).build();

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("ALREADY_APP", "TO_CHECK", "CONS", "SUSP_PREV", "REJ"))
                .build();

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        RewardTransaction alreadyApproved = RewardTransaction.builder()
                .id("ALREADY_APP")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(10L).build()))
                .build();

        RewardTransaction toCheck = RewardTransaction.builder()
                .id("TO_CHECK")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(20L).build()))
                .build();

        RewardTransaction consultable = RewardTransaction.builder()
                .id("CONS")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(30L).build()))
                .build();

        RewardTransaction suspendedPrev = RewardTransaction.builder()
                .id("SUSP_PREV")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(40L).build()))
                .build();

        RewardTransaction rejected = RewardTransaction.builder()
                .id("REJ")
                .initiatives(INITIATIVES_ID)
                .merchantId(MERCHANT_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build()))
                .build();
        
        when(rewardTransactionRepository.updateStatusAndReturnOld(INITIATIVE_ID, BATCH_ID, "ALREADY_APP", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(alreadyApproved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(INITIATIVE_ID, BATCH_ID, "TO_CHECK", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(toCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(INITIATIVE_ID, BATCH_ID, "CONS", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(consultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(INITIATIVE_ID, BATCH_ID, "SUSP_PREV", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(suspendedPrev));
        when(rewardTransactionRepository.updateStatusAndReturnOld(INITIATIVE_ID, BATCH_ID, "REJ", RewardBatchTrxStatus.APPROVED, null, batchMonth, null))
                .thenReturn(Mono.just(rejected));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID)
                .merchantId(MERCHANT_ID).initiativeId(INITIATIVE_ID).build();
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any(BatchCountersDTO.class)))
                .thenReturn(Mono.just(updated));

        StepVerifier.create(service.approvedTransactions(BATCH_ID, req, INITIATIVE_ID))
                .expectNext(updated)
                .verifyComplete();
    }

    @ParameterizedTest(name = "{0}: {1} -> {2}")
    @MethodSource("decisionCounterCases")
    void transactionDecisions_applyCharacterizedCounterDeltas(
            String decision,
            RewardBatchTrxStatus oldStatus,
            RewardBatchTrxStatus newStatus,
            CounterExpectation expected
    ) {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.EVALUATING)
                .month(batchMonth)
                .build();
        RewardTransaction transaction = RewardTransaction.builder()
                .id("transaction")
                .rewardBatchTrxStatus(oldStatus)
                .rewardBatchLastMonthElaborated("2025-11")
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();
        ChecksErrorDTO checksError = new ChecksErrorDTO();
        checksError.setCfError(true);
        TransactionsRequest request = TransactionsRequest.builder()
                .transactionIds(List.of(transaction.getId()))
                .reason("reason")
                .checksError("suspend".equals(decision) ? checksError : null)
                .build();

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING
        )).thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.updateStatusAndReturnOld(
                eq(INITIATIVE_ID),
                eq(BATCH_ID),
                eq(transaction.getId()),
                eq(newStatus),
                nullable(it.gov.pagopa.idpay.transactions.dto.ReasonDTO.class),
                eq(batchMonth),
                nullable(ChecksError.class)
        )).thenReturn(Mono.just(transaction));
        when(rewardBatchRepository.updateTotals(
                eq(INITIATIVE_ID),
                eq(BATCH_ID),
                any(BatchCountersDTO.class)
        )).thenReturn(Mono.just(batch));

        Mono<RewardBatch> result = switch (decision) {
            case "approve" -> service.approvedTransactions(BATCH_ID, request, INITIATIVE_ID);
            case "reject" -> service.rejectTransactions(BATCH_ID, INITIATIVE_ID, request);
            case "suspend" -> service.suspendTransactions(BATCH_ID, INITIATIVE_ID, request);
            default -> throw new IllegalArgumentException("Unsupported decision " + decision);
        };

        StepVerifier.create(result)
                .expectNext(batch)
                .verifyComplete();

        org.mockito.ArgumentCaptor<BatchCountersDTO> countersCaptor =
                org.mockito.ArgumentCaptor.forClass(BatchCountersDTO.class);
        verify(rewardBatchRepository).updateTotals(
                eq(INITIATIVE_ID),
                eq(BATCH_ID),
                countersCaptor.capture()
        );
        assertCounterExpectation(countersCaptor.getValue(), expected);
    }

    private static Stream<Arguments> decisionCounterCases() {
        CounterExpectation unchanged = new CounterExpectation(0, 0, 0, 0, 0);
        return Stream.of(
                Arguments.of("approve", RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.APPROVED, unchanged),
                Arguments.of("approve", RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.APPROVED, new CounterExpectation(1, 0, 0, 0, 0)),
                Arguments.of("approve", RewardBatchTrxStatus.CONSULTABLE, RewardBatchTrxStatus.APPROVED, new CounterExpectation(1, 0, 0, 0, 0)),
                Arguments.of("approve", RewardBatchTrxStatus.SUSPENDED, RewardBatchTrxStatus.APPROVED, new CounterExpectation(0, -1, 0, 100, -100)),
                Arguments.of("approve", RewardBatchTrxStatus.REJECTED, RewardBatchTrxStatus.APPROVED, new CounterExpectation(0, 0, -1, 100, 0)),
                Arguments.of("reject", RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED, new CounterExpectation(0, 0, 1, -100, 0)),
                Arguments.of("reject", RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.REJECTED, new CounterExpectation(1, 0, 1, -100, 0)),
                Arguments.of("reject", RewardBatchTrxStatus.CONSULTABLE, RewardBatchTrxStatus.REJECTED, new CounterExpectation(1, 0, 1, -100, 0)),
                Arguments.of("reject", RewardBatchTrxStatus.SUSPENDED, RewardBatchTrxStatus.REJECTED, new CounterExpectation(0, -1, 1, 0, -100)),
                Arguments.of("reject", RewardBatchTrxStatus.REJECTED, RewardBatchTrxStatus.REJECTED, unchanged),
                Arguments.of("suspend", RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.SUSPENDED, new CounterExpectation(0, 1, 0, -100, 100)),
                Arguments.of("suspend", RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.SUSPENDED, new CounterExpectation(1, 1, 0, -100, 100)),
                Arguments.of("suspend", RewardBatchTrxStatus.CONSULTABLE, RewardBatchTrxStatus.SUSPENDED, new CounterExpectation(1, 1, 0, -100, 100)),
                Arguments.of("suspend", RewardBatchTrxStatus.SUSPENDED, RewardBatchTrxStatus.SUSPENDED, new CounterExpectation(1, 0, 0, 0, 0)),
                Arguments.of("suspend", RewardBatchTrxStatus.REJECTED, RewardBatchTrxStatus.SUSPENDED, new CounterExpectation(0, 1, -1, 0, 100))
        );
    }

    private static void assertCounterExpectation(BatchCountersDTO actual, CounterExpectation expected) {
        assertEquals(expected.elaborated(), actual.getTrxElaborated());
        assertEquals(expected.suspended(), actual.getTrxSuspended());
        assertEquals(expected.rejected(), actual.getTrxRejected());
        assertEquals(expected.approvedAmount(), actual.getApprovedAmountCents());
        assertEquals(expected.suspendedAmount(), actual.getSuspendedAmountCents());
    }

    private record CounterExpectation(
            long elaborated,
            long suspended,
            long rejected,
            long approvedAmount,
            long suspendedAmount
    ) {
    }

    @Test
    void approvedTransactions_batchNotFoundOrInvalidState() {
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("t1")).build();

        StepVerifier.create(service.approvedTransactions(BATCH_ID, req, INITIATIVE_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void evaluatingRewardBatches_nullList_processesAllSent() {
        RewardBatch sent = RewardBatch.builder().id("S1")
                .status(RewardBatchStatus.SENT)
                .initialAmountCents(100L)
                .suspendedAmountCents(0L).build();

        when(rewardBatchRepository.findByStatusAndInitiativeId(RewardBatchStatus.SENT, INITIATIVE_ID)).thenReturn(Flux.just(sent));
        when(rewardTransactionRepository.rewardTransactionsByBatchIdAndInitiativeId("S1", INITIATIVE_ID)).thenReturn(Mono.empty());
        when(rewardTransactionRepository.sumSuspendedAccruedRewardCents(INITIATIVE_ID, "S1")).thenReturn(Mono.just(20L));
        when(rewardBatchRepository.updateStatusAndApprovedAmountCents("S1", RewardBatchStatus.EVALUATING, 100L, INITIATIVE_ID))
                .thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(null, INITIATIVE_ID))
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void evaluatingRewardBatches_withList_handlesMissingIdsAsEmpty() {
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus("S1", INITIATIVE_ID, RewardBatchStatus.SENT)).thenReturn(Mono.empty());

        StepVerifier.create(service.evaluatingRewardBatches(List.of("S1"), INITIATIVE_ID))
                .expectNext(0L)
                .verifyComplete();

        verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), anyLong(), eq(INITIATIVE_ID));
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
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID)).thenReturn(Mono.empty());

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
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.downloadApprovedRewardBatchFile(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchNotApprovedException.class)
                .verify();
    }

    @Test
    void downloadApprovedRewardBatchFile_allowed_whenRefundState() {

        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.PENDING_REFUND)
                .filename("file.csv")
                .merchantId(MERCHANT_ID)
                .build();

        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID))
                .thenReturn(Mono.just(batch));

        when(approvedRewardBatchBlobService.getFileSignedUrl(anyString()))
                .thenReturn("signed-refund");

        StepVerifier.create(
                        service.downloadApprovedRewardBatchFile(
                                MERCHANT_ID,
                                OP1,
                                INITIATIVE_ID,
                                BATCH_ID
                        )
                )
                .assertNext(r -> assertEquals("signed-refund", r.getApprovedBatchUrl()))
                .verifyComplete();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "   "})
    void downloadApprovedRewardBatchFile_missingFilename(String invalid) {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename(invalid).merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.downloadApprovedRewardBatchFile(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchMissingFilenameException.class)
                .verify();
    }

    @Test
    void downloadApprovedRewardBatchFile_success_merchant() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();

        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID)).thenReturn(Mono.just(batch));
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
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void rewardBatchConfirmation_invalidState() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(rb));

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void rewardBatchConfirmation_previousNotApproved_blocks() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3)
                .merchantId(MERCHANT_ID).initiativeId(INITIATIVE_ID).posType(PHYSICAL).month("2025-12").build();

        RewardBatch prev = RewardBatch.builder().id("P1").status(RewardBatchStatus.SENT).build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchListPort.findBatchesBeforeMonth(MERCHANT_ID, INITIATIVE_ID, PHYSICAL, "2025-12"))
                .thenReturn(Flux.just(prev));

        StepVerifier.create(service.rewardBatchConfirmation(INITIATIVE_ID, BATCH_ID))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void rewardBatchConfirmation_previousInRefundedState() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3)
                .merchantId(MERCHANT_ID).posType(PHYSICAL).month("2025-12").initiativeId(INITIATIVE_ID).build();

        RewardBatch prevApproved = RewardBatch.builder().id("P1").status(RewardBatchStatus.PENDING_REFUND).build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchListPort.findBatchesBeforeMonth(MERCHANT_ID, INITIATIVE_ID, PHYSICAL, "2025-12"))
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
    void rewardBatchConfirmation_emptyEvaluatingL3Batch_movesToApproving() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING)
                .assigneeLevel(RewardBatchAssignee.L3).numberOfTransactions(0L).initialAmountCents(0L)
                .approvedAmountCents(0L).suspendedAmountCents(0L).merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID).posType(PHYSICAL).month("2025-12").build();

        RewardBatch prevApproved = RewardBatch.builder().id("P1").status(RewardBatchStatus.APPROVED).build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchListPort.findBatchesBeforeMonth(MERCHANT_ID, INITIATIVE_ID, PHYSICAL, "2025-12"))
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
        RewardBatch b1 = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).build();
        RewardBatch b2 = RewardBatch.builder().id(BATCH_ID_2).merchantId(MERCHANT_ID).build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(b1));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID_2, INITIATIVE_ID))
                .thenReturn(Mono.just(b2));

        doReturn(Mono.just(b1)).when(serviceSpy).processSingleBatchConfirmation(b1, INITIATIVE_ID);
        doReturn(Mono.just(b2)).when(serviceSpy).processSingleBatchConfirmation(b2, INITIATIVE_ID);

        StepVerifier.create(serviceSpy.rewardBatchConfirmationBatch(INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2)))
                .verifyComplete();

        verify(rewardBatchRepository).findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchRepository).findRewardBatchByIdAndInitiativeId(BATCH_ID_2, INITIATIVE_ID);

        verify(serviceSpy).processSingleBatchConfirmation(b1, INITIATIVE_ID);
        verify(serviceSpy).processSingleBatchConfirmation(b2, INITIATIVE_ID);

        verify(rewardBatchRepository, never()).findByStatusAndInitiativeId(any(), any());
    }

    @Test
    void rewardBatchConfirmationBatch_emptyList_fetchesApprovingAndProcesses() {
        RewardBatch b1 = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.APPROVING)
                .build();

        when(rewardBatchRepository.findByStatusAndInitiativeId(
                eq(RewardBatchStatus.APPROVING), eq(INITIATIVE_ID), any(Pageable.class)))
                .thenReturn(Flux.just(b1), Flux.empty());

        doReturn(Mono.just(b1)).when(serviceSpy).processSingleBatchConfirmation(b1, INITIATIVE_ID);

        StepVerifier.create(serviceSpy.rewardBatchConfirmationBatch(
                        INITIATIVE_ID, Collections.emptyList()))
                .verifyComplete();

        verify(serviceSpy).processSingleBatchConfirmation(b1, INITIATIVE_ID);
        verify(rewardBatchRepository, times(2))
                .findByStatusAndInitiativeId(eq(RewardBatchStatus.APPROVING), eq(INITIATIVE_ID), any(Pageable.class));
    }

    @Test
    void processBatchesOrchestrator_shouldContinueOnSingleBatchError() {
        RewardBatch b1 = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).build();
        RewardBatch b2 = RewardBatch.builder().id(BATCH_ID_2).merchantId(MERCHANT_ID).build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(b1));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID_2, INITIATIVE_ID))
                .thenReturn(Mono.just(b2));

        doReturn(Mono.error(new RuntimeException("Error Batch 1")))
                .when(serviceSpy).processSingleBatchDelivery(b1, INITIATIVE_ID);
        doReturn(Mono.just(b2))
                .when(serviceSpy).processSingleBatchDelivery(b2, INITIATIVE_ID);

        StepVerifier.create(serviceSpy.rewardBatchDeliveryBatch(
                        INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2)))
                .verifyComplete();

        verify(serviceSpy).processSingleBatchDelivery(b1, INITIATIVE_ID);
        verify(serviceSpy).processSingleBatchDelivery(b2, INITIATIVE_ID);
    }

    @Test
    void rewardBatchDeliveryBatch_Success() {
        String initiativeId = "INITIATIVE_ID";
        String batchId = "BATCH_1";
        String fiscalCode = "FISCAL_123";

        RewardBatch batch = new RewardBatch();
        batch.setId(batchId);
        batch.setMerchantId(MERCHANT_ID);
        batch.setStatus(RewardBatchStatus.APPROVED);
        batch.setApprovedAmountCents(1000L);

        MerchantDetailDTO merchantDetail = new MerchantDetailDTO();
        merchantDetail.setFiscalCode(fiscalCode);
        merchantDetail.setVatNumber("VAT_123");

        InstitutionDTO inst = new InstitutionDTO();
        inst.setZipCode("00100");
        inst.setDigitalAddress("pec@test.it");
        InstitutionList instList = new InstitutionList(List.of(inst));

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(batchId, INITIATIVE_ID)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                batchId, initiativeId, RewardBatchStatus.APPROVED
        )).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, initiativeId)).thenReturn(Mono.just(merchantDetail));
        when(selfcareInstitutionsRestClient.getInstitutions(fiscalCode)).thenReturn(Mono.just(instList));
        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class))).thenReturn(Mono.empty());

        StepVerifier.create(serviceSpy.rewardBatchDeliveryBatch(initiativeId, List.of(batchId)))
                .verifyComplete();

        verify(erogazioniRestClient).postErogazione(argThat(req ->
                req.getId().equals(batchId) && req.getAnagrafica().getCap().equals("00100")
        ));
    }

    @Test
    void rewardBatchDeliveryBatch_Fail_MultipleInstitutions() {
        // Given
        String initiativeId = "INITIATIVE_ID";
        String batchId = "BATCH_1";
        String fiscalCode = "FISCAL_123";

        RewardBatch batch = new RewardBatch();
        batch.setId(batchId);
        batch.setMerchantId(MERCHANT_ID);
        batch.setStatus(RewardBatchStatus.APPROVED);
        batch.setApprovedAmountCents(10000L);

        MerchantDetailDTO merchantDetail = new MerchantDetailDTO();
        merchantDetail.setFiscalCode(fiscalCode);
        merchantDetail.setVatNumber("VAT123");
        merchantDetail.setBusinessName("Business");
        merchantDetail.setIban("IT00TEST");
        merchantDetail.setIbanHolder("Holder");

        InstitutionList instList = new InstitutionList(List.of(new InstitutionDTO(), new InstitutionDTO()));

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(batchId, INITIATIVE_ID)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                batchId, initiativeId, RewardBatchStatus.APPROVED
        )).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(merchantRestClient.getMerchantDetail(anyString(), anyString())).thenReturn(Mono.just(merchantDetail));
        when(selfcareInstitutionsRestClient.getInstitutions(fiscalCode)).thenReturn(Mono.just(instList));

        StepVerifier.create(service.rewardBatchDeliveryBatch(initiativeId, List.of(batchId)))
                .verifyComplete();

        verify(erogazioniRestClient, never()).postErogazione(any());
    }

    @Test
    void rewardBatchDeliveryBatch_Fail_ApprovedAmountZero() {
        // Given
        String initiativeId = "INIT_1";
        String batchId = "BATCH_1";

        RewardBatch batch = new RewardBatch();
        batch.setId(batchId);
        batch.setMerchantId("M1");
        batch.setStatus(RewardBatchStatus.APPROVED);
        batch.setApprovedAmountCents(0L);

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(batchId, initiativeId)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                batchId, initiativeId, RewardBatchStatus.APPROVED
        )).thenReturn(Mono.just(batch));

        StepVerifier.create(service.rewardBatchDeliveryBatch(initiativeId, List.of(batchId)))
                .verifyComplete();

        verify(erogazioniRestClient, never()).postErogazione(any());
        verify(merchantRestClient, never()).getMerchantDetail(any(), anyString());
        verify(selfcareInstitutionsRestClient, never()).getInstitutions(any());
    }

    @Test
    void handleSuspendedTransactions_nullOrZero_returnsOriginal() {
        RewardBatch rbNull = RewardBatch.builder().id(BATCH_ID).numberOfTransactionsSuspended(null).build();
        RewardBatch rbZero = RewardBatch.builder().id(BATCH_ID).numberOfTransactionsSuspended(0L).build();

        Mono<RewardBatch> r1 = ReflectionTestUtils.invokeMethod(serviceSpy, "handleSuspendedTransactions", rbNull, INITIATIVE_ID);
        Mono<RewardBatch> r2 = ReflectionTestUtils.invokeMethod(serviceSpy, "handleSuspendedTransactions", rbZero, INITIATIVE_ID);

        assertNotNull(r1);
        StepVerifier.create(r1).expectNext(rbNull).verifyComplete();
        assertNotNull(r2);
        StepVerifier.create(r2).expectNext(rbZero).verifyComplete();
    }

    @Test
    void addOneMonth_and_italian() {
        assertEquals("2026-01", service.addOneMonth("2025-12"));
        assertEquals("gennaio 2026", service.addOneMonthToItalian("dicembre 2025"));
    }

    @Test
    void updateAndSaveRewardTransactionsToApprove_setsApprovedAndSaves() {
        RewardBatch approving = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVING)
                .assigneeLevel(RewardBatchAssignee.L3).build();
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.APPROVING
        )).thenReturn(Mono.just(approving));
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList()))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.updateAndSaveRewardTransactionsToApprove(BATCH_ID, INITIATIVE_ID))
                .verifyComplete();
    }

    @Test
    void validateRewardBatch_notFound() {
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(RewardBatchNotFound.class)
                .verify();
    }

    @Test
    void validateRewardBatch_L1_to_L2_success() {
        RewardBatch b = RewardBatch.builder()
                .id(BATCH_ID)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));
        when(rewardTransactionRepository.findByRewardBatchIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Flux.concat(
                        Flux.range(1, 3).map(index -> RewardTransaction.builder()
                                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).build()),
                        Flux.range(1, 17).map(index -> RewardTransaction.builder()
                                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE).build())
                ));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> assertEquals(RewardBatchAssignee.L2, updated.getAssigneeLevel()))
                .verifyComplete();
    }

    @Test
    void validateRewardBatch_L1_to_L2_successWithZeroTransactionRows() {
        RewardBatch b = RewardBatch.builder()
                .id(BATCH_ID)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));
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
                .build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.validateRewardBatch("guest", INITIATIVE_ID, BATCH_ID))
                .expectError(RoleNotAllowedForL1PromotionException.class)
                .verify();
    }

    @Test
    void validateRewardBatch_L1_not15percent() {
        RewardBatch b = RewardBatch.builder()
                .id(BATCH_ID)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));
        when(rewardTransactionRepository.findByRewardBatchIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Flux.concat(
                        Flux.range(1, 2).map(index -> RewardTransaction.builder()
                                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED).build()),
                        Flux.range(1, 18).map(index -> RewardTransaction.builder()
                                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE).build())
                ));

        StepVerifier.create(service.validateRewardBatch(OP1, INITIATIVE_ID, BATCH_ID))
                .expectError(BatchNotElaborated15PercentException.class)
                .verify();
    }

    @Test
    void validateRewardBatch_L2_to_L3_success() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.validateRewardBatch(OP2, INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> assertEquals(RewardBatchAssignee.L3, updated.getAssigneeLevel()))
                .verifyComplete();
    }

    @Test
    void validateRewardBatch_L2_wrongRole() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));

        StepVerifier.create(service.validateRewardBatch("guest", INITIATIVE_ID, BATCH_ID))
                .expectError(RoleNotAllowedForL2PromotionException.class)
                .verify();
    }

    @Test
    void validateRewardBatch_invalidState() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L3).build();
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID)).thenReturn(Mono.just(b));

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
                .trxChargeDate(LocalDateTime.of(2025, Month.DECEMBER, 10, 10, 30))
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
    void mapTransactionToCsvRow_nullAdditionalProperties_productInfoIsNewline() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1").fiscalCode("CF").trxCode("CODE")
                .effectiveAmountCents(1000L)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(500L).build()))
                .additionalProperties(null)
                .invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder().filename("f.pdf").build())
                .build();

        String row = ReflectionTestUtils.invokeMethod(service, "mapTransactionToCsvRow", trx, INITIATIVE_ID);
        // productInfo is the second field
        String productInfoField = row.split(";")[1];
        assertEquals("\"\n\"", productInfoField);
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
        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
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

        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
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

        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
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

        LocalDate initiativeEnd = LocalDate.of(2026, Month.JANUARY, 6);
        InitiativeDetailDTO detail = new InitiativeDetailDTO();
        detail.setFruitionEndDate(initiativeEnd);

        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(initiativeDataService.getInitiativeData(eq(INITIATIVE_ID)))
                .thenReturn(Mono.just(detail));

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void postponeTransaction_initiativeNotFound_mapsToClientExceptionWithBody() {
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

        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(initiativeDataService.getInitiativeData(INITIATIVE_ID))
                .thenReturn(Mono.error(new InitiativeNotFoundException("not found")));

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
                .expectErrorSatisfies(error -> {
                    assertInstanceOf(ClientExceptionWithBody.class, error);
                    ClientExceptionWithBody exception = (ClientExceptionWithBody) error;
                    assertEquals(HttpStatus.NOT_FOUND, exception.getHttpStatus());
                    assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, exception.getCode());
                })
                .verify();

        verify(rewardBatchRepository, never())
                .findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(anyString(), anyString(), any(), anyString());
    }

    @Test
    void postponeTransaction_initiativeDataGenericError_mapsToInternalServerError() {
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

        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(initiativeDataService.getInitiativeData(INITIATIVE_ID))
                .thenReturn(Mono.error(new RuntimeException("boom")));

        StepVerifier.create(service.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
                .expectErrorSatisfies(error -> {
                    assertInstanceOf(ClientExceptionWithBody.class, error);
                    ClientExceptionWithBody exception = (ClientExceptionWithBody) error;
                    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getHttpStatus());
                    assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, exception.getCode());
                    assertTrue(exception.getMessage().contains("Failed to retrieve initiative data"));
                })
                .verify();

        verify(rewardBatchRepository, never())
                .findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(anyString(), anyString(), any(), anyString());
    }

    @Test
    void postponeTransaction_nextBatchInvalidStatus() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-01")
                .status(RewardBatchStatus.CREATED)
                .build();

        RewardBatch next = RewardBatch.builder()
                .id(BATCH_ID_2)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .status(RewardBatchStatus.SENT)
                .build();

        InitiativeDetailDTO detail = new InitiativeDetailDTO();
        detail.setFruitionEndDate(LocalDate.of(2027, Month.JANUARY, 1));

        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(initiativeDataService.getInitiativeData(INITIATIVE_ID))
                .thenReturn(Mono.just(detail));
        doReturn(Mono.just(next)).when(serviceSpy)
                .findOrCreateBatch(INITIATIVE_ID, MERCHANT_ID, PHYSICAL, "2026-02", BUSINESS_NAME);

        StepVerifier.create(serviceSpy.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
                .expectError(ClientExceptionNoBody.class)
                .verify();

        verify(rewardBatchRepository, never()).updateTotals(anyString(), anyString(), any());
        verify(rewardTransactionRepository, never()).save(any());
    }

    @Test
    void postponeTransaction_success_movesAndUpdatesTrx() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("T1")
                .merchantId(MERCHANT_ID)
                .initiatives(INITIATIVES_ID)
                .rewardBatchId(BATCH_ID)
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardBatch current = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-01")
                .status(RewardBatchStatus.CREATED)
                .build();

        RewardBatch next = RewardBatch.builder()
                .id(BATCH_ID_2)
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .businessName(BUSINESS_NAME)
                .posType(PHYSICAL)
                .month("2026-02")
                .status(RewardBatchStatus.CREATED)
                .build();
        
        InitiativeDetailDTO detail = new InitiativeDetailDTO();
        detail.setFruitionEndDate(LocalDate.of(2027, Month.JANUARY, 1));
        
        when(rewardTransactionRepository.findTransactionInBatch(INITIATIVE_ID, MERCHANT_ID, BATCH_ID, "T1"))
                .thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(initiativeDataService.getInitiativeData(eq(INITIATIVE_ID)))
                .thenReturn(Mono.just(detail));

        doReturn(Mono.just(next)).when(serviceSpy).findOrCreateBatch(INITIATIVE_ID, MERCHANT_ID, PHYSICAL, "2026-02", BUSINESS_NAME);

        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID), any()))
                .thenReturn(Mono.empty());

        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID_2), any()))
                .thenReturn(Mono.empty());


        StepVerifier.create(serviceSpy.postponeTransaction(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1"))
                .verifyComplete();

        verify(rewardBatchRepository, times(2))
                .updateTotals(anyString(), anyString(), any());



        assertEquals(BATCH_ID_2, trx.getRewardBatchId());
        assertNotNull(trx.getRewardBatchInclusionDate());
        assertNotNull(trx.getUpdateDate());
    }

    @Test
    void postponeTransaction_whenTransactionIsSuspended_shouldUpdateSuspendedCounters() {

        String merchantId = MERCHANT_ID;
        String initiativeId = INITIATIVE_ID;
        String rewardBatchId = BATCH_ID;
        String transactionId = "TRX_ID";

        long accruedRewardCents = 100L;

        RewardTransaction trx = new RewardTransaction();
        trx.setId(transactionId);
        trx.setRewardBatchId(rewardBatchId);
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);

        Reward reward = new Reward();
        reward.setAccruedRewardCents(accruedRewardCents);

        Map<String, Reward> rewardsMap = new HashMap<>();
        rewardsMap.put(initiativeId, reward);
        trx.setRewards(rewardsMap);

        RewardBatch currentBatch = new RewardBatch();
        currentBatch.setId(rewardBatchId);
        currentBatch.setStatus(RewardBatchStatus.CREATED);
        currentBatch.setInitiativeId(INITIATIVE_ID);
        currentBatch.setMonth("2026-01");
        currentBatch.setMerchantId(MERCHANT_ID);
        currentBatch.setBusinessName(BUSINESS_NAME);

        RewardBatch nextBatch = new RewardBatch();
        nextBatch.setId(BATCH_ID_2);
        nextBatch.setMerchantId(MERCHANT_ID);
        nextBatch.setInitiativeId(INITIATIVE_ID);
        nextBatch.setStatus(RewardBatchStatus.CREATED);

        when(rewardTransactionRepository.findTransactionInBatch(initiativeId, merchantId, rewardBatchId, transactionId))
                .thenReturn(Mono.just(trx));

        when(rewardBatchRepository.findById(rewardBatchId))
                .thenReturn(Mono.just(currentBatch));

        doReturn(Mono.just(nextBatch)).when(serviceSpy)
                .findOrCreateBatch(any(), any(), any(), any(), any());

        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(rewardBatchId), any()))
                .thenReturn(Mono.empty());

        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(BATCH_ID_2), any()))
                .thenReturn(Mono.empty());

        when(rewardTransactionRepository.save(any()))
                .thenReturn(Mono.just(trx));

        InitiativeDetailDTO detail = new InitiativeDetailDTO();
        detail.setFruitionEndDate(LocalDate.of(2027, Month.JANUARY, 1));
        when(initiativeDataService.getInitiativeData(eq(INITIATIVE_ID)))
                .thenReturn(Mono.just(detail));

        StepVerifier.create(
                        serviceSpy.postponeTransaction(
                                merchantId,
                                INITIATIVE_ID,
                                rewardBatchId,
                                transactionId
                        )
                )
                .verifyComplete();

        verify(rewardBatchRepository, times(2))
                .updateTotals(eq(INITIATIVE_ID), anyString(), any(BatchCountersDTO.class));

        verify(rewardTransactionRepository).save(trx);
    }

    @Test
    void checkRewardBatchesOutcomes_withIds_success() {

        RewardBatch batch1 = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        RewardBatch batch2 = RewardBatch.builder().id(BATCH_ID_2).status(RewardBatchStatus.PENDING_REFUND).build();

        ErogazioneOutcomeDTO erogazione1 = ErogazioneOutcomeDTO.builder()
                .status("COMPLETATA")
                .dateValue(LocalDate.now())
                .build();

        InvitaliaOutcomeResponseDTO outcome1 = InvitaliaOutcomeResponseDTO.builder()
                .message(null)
                .erogazione(erogazione1)
                .build();

        ErogazioneOutcomeDTO erogazione2 = ErogazioneOutcomeDTO.builder()
                .status("RIFIUTATA")
                .build();

        ErrorInvitaliaDTO error = new ErrorInvitaliaDTO("ERR01", "Errore");

        InvitaliaOutcomeResponseDTO outcome2 = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione2)
                .errors(List.of(error))
                .build();

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.PENDING_REFUND))
                .thenReturn(Mono.just(batch1));
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(BATCH_ID_2, INITIATIVE_ID, RewardBatchStatus.PENDING_REFUND))
                .thenReturn(Mono.just(batch2));

        when(erogazioniRestClient.getOutcome(BATCH_ID)).thenReturn(Mono.just(outcome1));
        when(erogazioniRestClient.getOutcome(BATCH_ID_2)).thenReturn(Mono.just(outcome2));

        when(rewardBatchRepository.save(any(RewardBatch.class)))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, null)).thenReturn(Mono.just(batch1));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID_2, null)).thenReturn(Mono.just(batch2));

        StepVerifier.create(service.checkRewardBatchesOutcomes(INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2)))
                .verifyComplete();

        assertEquals(RewardBatchStatus.REFUNDED, batch1.getStatus());
        assertNotNull(batch1.getRefundValutaDate());

        assertEquals(RewardBatchStatus.NOT_REFUNDED, batch2.getStatus());
        assertEquals("ERR01 - Errore", batch2.getRefundErrorMessage());

        verify(rewardBatchRepository).findByIdAndInitiativeIdAndStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.PENDING_REFUND);
        verify(rewardBatchRepository).findByIdAndInitiativeIdAndStatus(BATCH_ID_2, INITIATIVE_ID, RewardBatchStatus.PENDING_REFUND);
        verify(erogazioniRestClient).getOutcome(BATCH_ID);
        verify(erogazioniRestClient).getOutcome(BATCH_ID_2);
        verify(rewardBatchRepository, times(2)).save(any());
    }

    @Test
    void checkRewardBatchesOutcomes_emptyList_success() {
        RewardBatch batch1 = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("COMPLETATA")
                .dateValue(LocalDate.now())
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .message(null)
                .erogazione(erogazione)
                .build();

        when(rewardBatchRepository.findByStatusAndInitiativeId(RewardBatchStatus.PENDING_REFUND, INITIATIVE_ID))
                .thenReturn(Flux.just(batch1));

        when(erogazioniRestClient.getOutcome(BATCH_ID)).thenReturn(Mono.just(outcome));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, null)).thenReturn(Mono.just(batch1));

        StepVerifier.create(service.checkRewardBatchesOutcomes(INITIATIVE_ID, null))
                .verifyComplete();

        assertEquals(RewardBatchStatus.REFUNDED, batch1.getStatus());
        verify(rewardBatchRepository).findByStatusAndInitiativeId(RewardBatchStatus.PENDING_REFUND, INITIATIVE_ID);
        verify(erogazioniRestClient).getOutcome(BATCH_ID);
        verify(rewardBatchRepository).save(batch1);
    }

    @Test
    void updateBatch_completata_setsRefunded() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("COMPLETATA")
                .dateValue(LocalDate.now())
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .message(null)
                .erogazione(erogazione)
                .build();

        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, null)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> {
                    assertEquals(RewardBatchStatus.REFUNDED, b.getStatus());
                    assertNotNull(b.getRefundValutaDate());
                    assertNotNull(b.getRefundOutcomeTimestamp());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(batch);
    }

    @Test
    void updateBatch_rifiutata_setsNotRefunded_withoutErrors() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.PENDING_REFUND)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("RIFIUTATA")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .errors(null)
                .build();

        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, null)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> {
                    assertEquals(RewardBatchStatus.NOT_REFUNDED, b.getStatus());
                    assertNull(b.getRefundErrorMessage());
                    assertNotNull(b.getRefundOutcomeTimestamp());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(batch);
    }

    @Test
    void updateBatch_inLavorazione_withCreatedStatus_setsPendingRefundTimestamp() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.CREATED)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("IN_LAVORAZIONE")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .build();

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> assertEquals(RewardBatchStatus.CREATED, b.getStatus()))
                .verifyComplete();
    }

    @Test
    void updateBatch_errore_withCreatedStatus_setsPendingRefundTimestamp() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.CREATED)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("ERRORE")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .build();

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> assertEquals(RewardBatchStatus.CREATED, b.getStatus()))
                .verifyComplete();
    }

    @Test
    void updateBatch_rifiutata_withoutErrors_setsNotRefunded() {

        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.PENDING_REFUND)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("RIFIUTATA")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .errors(null)
                .build();

        when(rewardBatchRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, null)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> {
                    assertEquals(RewardBatchStatus.NOT_REFUNDED, b.getStatus());
                    assertNull(b.getRefundErrorMessage());
                })
                .verifyComplete();
    }

    @Test
    void updateBatch_rifiutata_withEmptyErrors_setsNotRefunded() {

        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.PENDING_REFUND)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("RIFIUTATA")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .errors(List.of())
                .build();

        when(rewardBatchRepository.save(any()))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, null)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> {
                    assertEquals(RewardBatchStatus.NOT_REFUNDED, b.getStatus());
                    assertNull(b.getRefundErrorMessage());
                })
                .verifyComplete();
    }

    @Test
    void updateBatch_errore_withPendingStatus_keepsPendingRefund() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.PENDING_REFUND)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("ERRORE")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .build();

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> assertEquals(RewardBatchStatus.PENDING_REFUND, b.getStatus()))
                .verifyComplete();
    }

    @Test
    void updateBatch_inLavorazione_withPendingStatus_keepsPendingRefund() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .status(RewardBatchStatus.PENDING_REFUND)
                .build();

        ErogazioneOutcomeDTO erogazione = ErogazioneOutcomeDTO.builder()
                .status("IN_LAVORAZIONE")
                .build();

        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder()
                .erogazione(erogazione)
                .build();

        StepVerifier.create(service.updateBatch(batch, outcome))
                .assertNext(b -> assertEquals(RewardBatchStatus.PENDING_REFUND, b.getStatus()))
                .verifyComplete();
    }

    @Test
    void getTargetMonth_whenOriginalMonthIsInFuture_returnsOriginalMonth() {
        String futureMonth = YearMonth.now().plusMonths(2).toString();

        String result = service.getTargetMonth(futureMonth);

        assertEquals(futureMonth, result);
    }

    @Test
    void getTargetMonth_whenOriginalMonthIsCurrent_returnsCurrentMonth() {
        String currentMonth = YearMonth.now().toString();

        String result = service.getTargetMonth(currentMonth);

        assertEquals(currentMonth, result);
    }

    @Test
    void getTargetMonth_whenOriginalMonthIsInPast_returnsCurrentMonth() {
        String pastMonth = YearMonth.now().minusMonths(2).toString();
        String currentMonth = YearMonth.now().toString();

        String result = service.getTargetMonth(pastMonth);

        assertEquals(currentMonth, result);
    }

    @Test
    void generateAndSaveCsv_batchNotFound_switchIfEmpty() {
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());
        Mono<String> generated = service.generateAndSaveCsv(BATCH_ID, INITIATIVE_ID, MERCHANT_ID);
        StepVerifier.create(generated)
                .expectErrorSatisfies(throwable -> {
                    assertInstanceOf(ClientExceptionWithBody.class, throwable);
                    ClientExceptionWithBody ex = (ClientExceptionWithBody) throwable;
                    assertEquals(HttpStatus.NOT_FOUND, ex.getHttpStatus());
                })
                .verify();
    }

    @Test
    void rewardBatchDeliveryBatch_withoutIds_shouldFetchOnlyApprovedBatchesWithPositiveAmount() {
        when(rewardBatchRepository.findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                eq(RewardBatchStatus.APPROVED),
                eq(INITIATIVE_ID),
                eq(0L),
                any(Pageable.class)
        )).thenReturn(Flux.empty());

        StepVerifier.create(service.rewardBatchDeliveryBatch(INITIATIVE_ID, null))
                .verifyComplete();

        verify(rewardBatchRepository).findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                eq(RewardBatchStatus.APPROVED),
                eq(INITIATIVE_ID),
                eq(0L),
                any(Pageable.class)
        );

        verify(rewardBatchRepository, never()).findByStatusAndInitiativeId(
                eq(RewardBatchStatus.APPROVED),
                eq(INITIATIVE_ID),
                any(Pageable.class)
        );
    }

    @Test
    void rewardBatchDeliveryBatch_withoutIds_shouldProcessFoundValidBatchAndThenStop() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.APPROVED)
                .approvedAmountCents(100L)
                .build();

        when(rewardBatchRepository.findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                eq(RewardBatchStatus.APPROVED),
                eq(INITIATIVE_ID),
                eq(0L),
                any(Pageable.class)
        ))
                .thenReturn(Flux.just(batch))
                .thenReturn(Flux.empty());

        doReturn(Mono.just(batch))
                .when(serviceSpy)
                .processSingleBatchDelivery(batch, INITIATIVE_ID);

        StepVerifier.create(serviceSpy.rewardBatchDeliveryBatch(INITIATIVE_ID, Collections.emptyList()))
                .verifyComplete();

        verify(rewardBatchRepository, times(2))
                .findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                        eq(RewardBatchStatus.APPROVED),
                        eq(INITIATIVE_ID),
                        eq(0L),
                        any(Pageable.class)
                );

        verify(serviceSpy).processSingleBatchDelivery(batch, INITIATIVE_ID);

        verify(rewardBatchRepository, never())
                .findByStatusAndInitiativeId(
                        eq(RewardBatchStatus.APPROVED),
                        eq(INITIATIVE_ID),
                        any(Pageable.class)
                );
    }

    @Test
    void rewardBatchDeliveryBatch_withoutIds_shouldNotProcessBatchesWithZeroAmount() {
        when(rewardBatchRepository.findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                eq(RewardBatchStatus.APPROVED),
                eq(INITIATIVE_ID),
                eq(0L),
                any(Pageable.class)
        )).thenReturn(Flux.empty());

        StepVerifier.create(
                serviceSpy.rewardBatchDeliveryBatch(
                        INITIATIVE_ID,
                        Collections.emptyList()
                )
        ).verifyComplete();

        verify(serviceSpy, never())
                .processSingleBatchDelivery(any(), anyString());

        verify(rewardBatchRepository)
                .findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                        eq(RewardBatchStatus.APPROVED),
                        eq(INITIATIVE_ID),
                        eq(0L),
                        any(Pageable.class)
                );
    }

    private RewardBatchServiceImpl serviceWithWorkerPorts() {
        return new RewardBatchServiceImpl(
                rewardBatchRepository,
                new MongoRewardBatchLifecycleAdapter(rewardBatchRepository),
                rewardBatchListPort,
                new MongoMerchantRewardBatchLookupAdapter(rewardBatchRepository),
                rewardTransactionRepository,
                new MongoRewardTransactionAdapter(rewardTransactionRepository, rewardBatchRepository),
                new MongoRewardTransactionAdapter(rewardTransactionRepository, rewardBatchRepository),
                new MongoRewardBatchTransactionMutationAdapter(rewardTransactionRepository, rewardBatchRepository),
                rewardBatchFinalApprovalPort,
                rewardBatchAssigneePromotionPort,
                rewardBatchDeliveryPort,
                suspendedTransactionReassignmentPort,
                userRestClient,
                approvedRewardBatchBlobService,
                checksErrorMapper,
                auditUtilities,
                merchantRestClient,
                selfcareInstitutionsRestClient,
                erogazioniRestClient,
                initiativeDataService,
                10
        );
    }
}
