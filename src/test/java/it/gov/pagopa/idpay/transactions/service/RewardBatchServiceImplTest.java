package it.gov.pagopa.idpay.transactions.service;

import com.azure.storage.blob.models.BlobStorageException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantRewardBatchLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantTransactionPostponementPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchAssigneePromotionPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchDeliveryPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchFinalApprovalPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchLifecyclePort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchListPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionDecisionPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.persistence.port.SuspendedTransactionReassignmentPort;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import java.time.LocalDate;
import java.time.YearMonth;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

    @Mock private RewardBatchLifecyclePort lifecyclePort;
    @Mock private RewardBatchListPort listPort;
    @Mock private MerchantRewardBatchLookupPort merchantLookupPort;
    @Mock private RewardBatchTransactionReadPort transactionReadPort;
    @Mock private RewardBatchTransactionDecisionPort decisionPort;
    @Mock private MerchantTransactionPostponementPort postponementPort;
    @Mock private RewardBatchFinalApprovalPort finalApprovalPort;
    @Mock private RewardBatchAssigneePromotionPort promotionPort;
    @Mock private RewardBatchDeliveryPort deliveryPort;
    @Mock private SuspendedTransactionReassignmentPort reassignmentPort;
    @Mock private UserRestClient userRestClient;
    @Mock private ApprovedRewardBatchBlobService batchBlobService;
    @Mock private ChecksErrorMapper checksErrorMapper;
    @Mock private AuditUtilities auditUtilities;
    @Mock private MerchantRestClient merchantRestClient;
    @Mock private SelfcareInstitutionsRestClient selfcareClient;
    @Mock private ErogazioniRestClient erogazioniClient;
    @Mock private InitiativeDataService initiativeDataService;
    private RewardBatchServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new RewardBatchServiceImpl(
                lifecyclePort, listPort, merchantLookupPort, transactionReadPort, decisionPort,
                postponementPort, finalApprovalPort, promotionPort, deliveryPort, reassignmentPort,
                userRestClient, batchBlobService, checksErrorMapper, auditUtilities, merchantRestClient,
                selfcareClient, erogazioniClient, initiativeDataService, 10);
    }

    @Test
    void decisionReturnsFreshSqlLifecycleAggregateAfterTransactionMutation() {
        RewardBatch evaluating = RewardBatch.builder()
                .id("batch").initiativeId("initiative").month("2026-01")
                .status(RewardBatchStatus.EVALUATING).build();
        RewardBatch refreshed = RewardBatch.builder()
                .id("batch").initiativeId("initiative").numberOfTransactionsElaborated(1L).build();
        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of("transaction")).build();
        when(lifecyclePort.findBatchWithStatus("batch", "initiative", RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(evaluating));
        when(decisionPort.updateStatusAndReturnOld(
                "initiative", "batch", "transaction", RewardBatchTrxStatus.APPROVED,
                null, "2026-01", null)).thenReturn(Mono.just(new RewardTransaction()));
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(refreshed));

        StepVerifier.create(service.approvedTransactions("batch", request, "initiative"))
                .assertNext(batch -> assertEquals(1L, batch.getNumberOfTransactionsElaborated()))
                .verifyComplete();

        verify(decisionPort).updateStatusAndReturnOld(
                "initiative", "batch", "transaction", RewardBatchTrxStatus.APPROVED,
                null, "2026-01", null);
        verify(lifecyclePort).findBatch("batch", "initiative");
    }

    @Test
    void evaluationDelegatesEachSentBatchToSqlDecisionPort() {
        RewardBatch sent = RewardBatch.builder().id("batch").build();
        when(lifecyclePort.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative"))
                .thenReturn(reactor.core.publisher.Flux.just(sent));
        when(decisionPort.prepareEvaluation("batch", "initiative")).thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(null, "initiative"))
                .expectNext(1L)
                .verifyComplete();

        verify(lifecyclePort).findBatchesWithStatus(RewardBatchStatus.SENT, "initiative");
        verify(lifecyclePort, never()).findBatchWithStatus(
                anyString(), eq("initiative"), eq(RewardBatchStatus.SENT));
        verify(decisionPort).prepareEvaluation("batch", "initiative");
    }

    @Test
    void evaluationWithEmptyExplicitIdsIsNoOpWithoutFallingBackToAllBatches() {
        StepVerifier.create(service.evaluatingRewardBatches(List.of(), "initiative"))
                .expectNext(0L)
                .verifyComplete();

        verifyNoInteractions(lifecyclePort, decisionPort);
    }

    @Test
    void postponementUsesSqlPortWithInitiativeFruitionBoundary() {
        LocalDate endDate = LocalDate.parse("2026-12-31");
        when(initiativeDataService.getInitiativeData("initiative"))
                .thenReturn(Mono.just(InitiativeDetailDTO.builder().fruitionEndDate(endDate).build()));
        when(postponementPort.postponeTransaction(
                "merchant", "initiative", "batch", "transaction", endDate))
                .thenReturn(Mono.just(new RewardTransaction()));

        StepVerifier.create(service.postponeTransaction("merchant", "initiative", "batch", "transaction"))
                .verifyComplete();

        verify(postponementPort).postponeTransaction(
                "merchant", "initiative", "batch", "transaction", endDate);
    }

    @Test
    void listsBatchesWithTheCallerVisibilityAndMatchingCount() {
        RewardBatch batch = batch("batch", RewardBatchStatus.CREATED);
        when(listPort.findRewardBatches(
                "merchant", "initiative", null, null, null, false, PageRequest.of(0, 10)))
                .thenReturn(Flux.just(batch));
        when(listPort.countRewardBatches("merchant", "initiative", null, null, null, false))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getRewardBatches(
                        "merchant", "initiative", "merchant", null, null, null, PageRequest.of(0, 10)))
                .assertNext(page -> assertEquals("batch", page.getContent().getFirst().getId()))
                .verifyComplete();
    }

    @Test
    void sendBatchRejectsMissingWrongMerchantAndInvalidLifecycleStates() {
        when(lifecyclePort.findBatch("missing")).thenReturn(Mono.empty());
        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "missing"))
                .expectError().verify();

        RewardBatch wrongMerchant = batch("wrong", RewardBatchStatus.CREATED);
        wrongMerchant.setMerchantId("other");
        when(lifecyclePort.findBatch("wrong")).thenReturn(Mono.just(wrongMerchant));
        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "wrong"))
                .expectError().verify();

        RewardBatch sent = batch("sent", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatch("sent")).thenReturn(Mono.just(sent));
        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "sent"))
                .expectError().verify();
    }

    @Test
    void sendHistoricalBatchPersistsSentAfterPreviousBatchesAreClear() {
        RewardBatch batch = batch("batch", RewardBatchStatus.CREATED);
        batch.setMonth(YearMonth.now().minusMonths(1).toString());
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(batch));
        when(lifecyclePort.findMerchantBatches("merchant", "initiative", PosType.PHYSICAL))
                .thenReturn(Flux.empty());
        when(lifecyclePort.saveBatch(batch)).thenReturn(Mono.just(batch));

        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "batch"))
                .verifyComplete();

        assertEquals(RewardBatchStatus.SENT, batch.getStatus());
        verify(lifecyclePort).saveBatch(batch);
    }

    @Test
    void decisionRequestsRejectInvalidChecksErrorsAndMissingEvaluatingBatch() {
        ChecksErrorDTO emptyChecks = new ChecksErrorDTO();
        TransactionsRequest invalid = TransactionsRequest.builder()
                .transactionIds(List.of("transaction")).checksError(emptyChecks).build();
        assertThrows(
                RuntimeException.class, () -> service.rejectTransactions("batch", "initiative", invalid));

        TransactionsRequest request = TransactionsRequest.builder().transactionIds(List.of("transaction")).build();
        when(lifecyclePort.findBatchWithStatus("batch", "initiative", RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());
        StepVerifier.create(service.suspendTransactions("batch", "initiative", request))
                .expectError().verify();
    }

    @Test
    void downloadApprovedFileAppliesHeaderRoleStatusAndFilenameRules() {
        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, null, "initiative", "batch"))
                .expectError().verify();
        RewardBatch evaluating = batch("batch", RewardBatchStatus.EVALUATING);
        when(merchantLookupPort.findMerchantBatch("merchant", "initiative", "batch"))
                .thenReturn(Mono.just(evaluating));
        StepVerifier.create(service.downloadApprovedRewardBatchFile(
                        "merchant", null, "initiative", "batch"))
                .expectError().verify();
        verifyNoInteractions(batchBlobService);

        RewardBatch approved = batch("approved", RewardBatchStatus.APPROVED);
        approved.setFilename("approved.csv");
        when(merchantLookupPort.findMerchantBatch("merchant", "initiative", "approved"))
                .thenReturn(Mono.just(approved));
        when(batchBlobService.getFileSignedUrl(
                "initiative/initiative/merchant/merchant/batch/approved/approved.csv"))
                .thenReturn(Mono.just("signed-url"));
        StepVerifier.create(service.downloadApprovedRewardBatchFile(
                        "merchant", null, "initiative", "approved"))
                .assertNext(response -> assertEquals("signed-url", response.getApprovedBatchUrl()))
                .verifyComplete();
    }

    @Test
    void confirmationChecksStateAndPersistsApprovingBatchWhenPreviousBatchesAreClear() {
        when(lifecyclePort.findBatch("missing", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(service.rewardBatchConfirmation("initiative", "missing"))
                .expectError().verify();

        RewardBatch evaluating = batch("batch", RewardBatchStatus.EVALUATING);
        evaluating.setAssigneeLevel(RewardBatchAssignee.L3);
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(evaluating));
        when(listPort.findBatchesBeforeMonth("merchant", "initiative", PosType.PHYSICAL, evaluating.getMonth()))
                .thenReturn(Flux.empty());
        when(lifecyclePort.saveBatch(evaluating)).thenReturn(Mono.just(evaluating));
        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .assertNext(result -> assertEquals(RewardBatchStatus.APPROVING, result.getStatus()))
                .verifyComplete();
    }

    @Test
    void promotionValidatesRoleAndDelegatesTheExpectedSqlTransition() {
        RewardBatch l1 = batch("batch", RewardBatchStatus.EVALUATING);
        l1.setAssigneeLevel(RewardBatchAssignee.L1);
        RewardBatch promoted = batch("batch", RewardBatchStatus.EVALUATING);
        promoted.setAssigneeLevel(RewardBatchAssignee.L2);
        when(promotionPort.findBatchForPromotion("batch", "initiative")).thenReturn(Mono.just(l1));
        when(promotionPort.promote("batch", "initiative", RewardBatchAssignee.L1, RewardBatchAssignee.L2))
                .thenReturn(Mono.just(promoted));
        StepVerifier.create(service.validateRewardBatch("operator1", "initiative", "batch"))
                .expectNext(promoted).verifyComplete();

        when(promotionPort.findBatchForPromotion("batch", "initiative")).thenReturn(Mono.just(l1));
        StepVerifier.create(service.validateRewardBatch("operator2", "initiative", "batch"))
                .expectError().verify();
    }

    @Test
    void csvGenerationRejectsUnsafeBatchIdsBeforeAccessingPersistence() {
        StepVerifier.create(service.generateAndSaveCsv("../batch", "initiative", "merchant"))
                .expectError(IllegalArgumentException.class).verify();
    }

    @Test
    void initiativeLookupFailuresAreMappedBeforePostponementPortAccess() {
        when(initiativeDataService.getInitiativeData("initiative"))
                .thenReturn(Mono.error(new IllegalStateException("unavailable")));
        StepVerifier.create(service.postponeTransaction("merchant", "initiative", "batch", "transaction"))
                .expectError().verify();
    }

    @Test
    void confirmationWorkerReassignsSuspendedRowsThenCompletesApproval() {
        RewardBatch approving = batch("batch", RewardBatchStatus.APPROVING);
        approving.setAssigneeLevel(RewardBatchAssignee.L3);
        RewardBatch prepared = batch("batch", RewardBatchStatus.APPROVING);
        prepared.setNumberOfTransactionsSuspended(1L);
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        RewardBatchServiceImpl worker = spy(service);
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(approving));
        when(finalApprovalPort.prepareFinalApproval("batch", "initiative")).thenReturn(Mono.just(prepared));
        when(reassignmentPort.reassignSuspendedTransactions("batch", "initiative")).thenReturn(Mono.empty());
        when(finalApprovalPort.completeFinalApproval("batch", "initiative")).thenReturn(Mono.just(approved));
        doReturn(Mono.just("approved.csv")).when(worker)
                .generateAndSaveCsv("batch", "initiative", "merchant");

        StepVerifier.create(worker.processSingleBatchConfirmation(approving, "initiative"))
                .expectNext(approved).verifyComplete();

        verify(reassignmentPort).reassignSuspendedTransactions("batch", "initiative");
        verify(finalApprovalPort).completeFinalApproval("batch", "initiative");
    }

    @Test
    void confirmationWorkerRejectsMissingOrInvalidLifecycleState() {
        RewardBatch invalid = batch("batch", RewardBatchStatus.CREATED);
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(invalid));
        StepVerifier.create(service.processSingleBatchConfirmation(invalid, "initiative"))
                .expectError().verify();
        when(lifecyclePort.findBatch("missing", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(service.processSingleBatchConfirmation(batch("missing", RewardBatchStatus.APPROVING), "initiative"))
                .expectError().verify();
    }

    @Test
    void refundOutcomesUseSqlDeliveryPortForCompletionAndRejection() {
        RewardBatch pending = batch("batch", RewardBatchStatus.PENDING_REFUND);
        RewardBatch refunded = batch("batch", RewardBatchStatus.REFUNDED);
        var completed = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("COMPLETATA").dateValue(LocalDate.parse("2026-01-02")).build()).build();
        when(deliveryPort.recordRefundOutcome(
                "batch", "initiative", RewardBatchStatus.REFUNDED, LocalDate.parse("2026-01-02"), null))
                .thenReturn(Mono.just(refunded));
        StepVerifier.create(service.updateBatch(pending, completed)).expectNext(refunded).verifyComplete();

        RewardBatch notRefunded = batch("batch", RewardBatchStatus.NOT_REFUNDED);
        var rejected = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("RIFIUTATA").build()).build();
        when(deliveryPort.recordRefundOutcome(
                "batch", "initiative", RewardBatchStatus.NOT_REFUNDED, null, null))
                .thenReturn(Mono.just(notRefunded));
        StepVerifier.create(service.updateBatch(pending, rejected)).expectNext(notRefunded).verifyComplete();
    }

    @Test
    void refundOutcomeStateMismatchAndNonTerminalOutcomesAreHandled() {
        RewardBatch pending = batch("batch", RewardBatchStatus.PENDING_REFUND);
        var completed = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("COMPLETATA").dateValue(LocalDate.parse("2026-01-02")).build()).build();
        when(deliveryPort.recordRefundOutcome(
                "batch", "initiative", RewardBatchStatus.REFUNDED, LocalDate.parse("2026-01-02"), null))
                .thenReturn(Mono.empty());
        StepVerifier.create(service.updateBatch(pending, completed)).expectError().verify();

        var processing = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("IN_LAVORAZIONE").build()).build();
        StepVerifier.create(service.updateBatch(pending, processing)).expectNext(pending).verifyComplete();
    }

    @Test
    void batchWorkersContinueWhenOneExplicitBatchFails() {
        when(lifecyclePort.findBatch("missing", "initiative")).thenReturn(Mono.empty());
        when(lifecyclePort.findBatch("valid", "initiative")).thenReturn(Mono.just(batch("valid", RewardBatchStatus.APPROVING)));
        StepVerifier.create(service.rewardBatchConfirmationBatch("initiative", List.of("missing", "valid")))
                .verifyComplete();
    }

    @Test
    void deliveryWorkerBuildsRequestFromSnapshotAndRecordsSqlOutcome() {
        RewardBatch requested = batch("batch", RewardBatchStatus.APPROVED);
        RewardBatch snapshotted = batch("batch", RewardBatchStatus.APPROVED);
        snapshotted.setDeliveryAmountCents(123L);
        snapshotted.setApprovalDate(java.time.LocalDateTime.parse("2026-01-01T10:00:00"));
        RewardBatch delivered = batch("batch", RewardBatchStatus.PENDING_REFUND);
        MerchantDetailDTO merchant = MerchantDetailDTO.builder()
                .fiscalCode("fiscal").vatNumber("vat").businessName("business")
                .iban("iban").ibanHolder("holder").build();
        InstitutionDTO institution = InstitutionDTO.builder()
                .zipCode("00100").address("street").city("Rome").county("RM")
                .digitalAddress("pec@example.com").build();
        DeliveryOutcomeDTO outcome = DeliveryOutcomeDTO.builder().succeded(true).message("accepted").build();
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(requested));
        when(deliveryPort.snapshotDeliveryAmount("batch", "initiative")).thenReturn(Mono.just(snapshotted));
        when(merchantRestClient.getMerchantDetail("merchant", "initiative")).thenReturn(Mono.just(merchant));
        when(selfcareClient.getInstitutions("fiscal")).thenReturn(Mono.just(new InstitutionList(List.of(institution))));
        when(erogazioniClient.postErogazione(any())).thenReturn(Mono.just(outcome));
        when(deliveryPort.recordDeliveryOutcome("batch", "initiative", outcome)).thenReturn(Mono.just(delivered));

        StepVerifier.create(service.processSingleBatchDelivery(requested, "initiative"))
                .expectNext(delivered).verifyComplete();

        verify(deliveryPort).recordDeliveryOutcome("batch", "initiative", outcome);
    }

    @Test
    void deliveryWorkerRejectsMissingSnapshotMerchantAndAmbiguousInstitutionData() {
        RewardBatch requested = batch("batch", RewardBatchStatus.APPROVED);
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(requested));
        when(deliveryPort.snapshotDeliveryAmount("batch", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(service.processSingleBatchDelivery(requested, "initiative")).expectError().verify();

        RewardBatch snapshotted = batch("batch", RewardBatchStatus.APPROVED);
        snapshotted.setDeliveryAmountCents(100L);
        when(deliveryPort.snapshotDeliveryAmount("batch", "initiative")).thenReturn(Mono.just(snapshotted));
        when(merchantRestClient.getMerchantDetail("merchant", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(service.processSingleBatchDelivery(requested, "initiative")).expectError().verify();
    }

    @Test
    void csvGenerationReadsApprovedTransactionsUploadsAndPersistsFilename() {
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        @SuppressWarnings("unchecked")
        com.azure.core.http.rest.Response<com.azure.storage.blob.models.BlockBlobItem> response =
                mock(com.azure.core.http.rest.Response.class);
        when(response.getStatusCode()).thenReturn(201);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        when(transactionReadPort.findBatchTransactions(
                "batch", "initiative", List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED)))
                .thenReturn(Flux.empty());
        when(batchBlobService.upload(
                any(InputStream.class), anyString(),
                anyString())).thenReturn(Mono.just(response));
        when(lifecyclePort.saveBatch(approved)).thenReturn(Mono.just(approved));

        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectNext("business_name_FISICO.csv").verifyComplete();

        assertEquals("business_name_FISICO.csv", approved.getFilename());
        verify(batchBlobService).upload(
                any(InputStream.class),
                contains("initiative/initiative/merchant/merchant/batch/batch/"),
                eq("text/csv; charset=UTF-8"));
    }

    @Test
    void csvGenerationSurfacesStorageFailureAndMissingBatch() {
        when(lifecyclePort.findBatch("missing")).thenReturn(Mono.empty());
        StepVerifier.create(service.generateAndSaveCsv("missing", "initiative", "merchant"))
                .expectError().verify();

        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        @SuppressWarnings("unchecked")
        com.azure.core.http.rest.Response<com.azure.storage.blob.models.BlockBlobItem> failedResponse =
                mock(com.azure.core.http.rest.Response.class);
        when(failedResponse.getStatusCode()).thenReturn(500);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        when(transactionReadPort.findBatchTransactions(
                "batch", "initiative", List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED)))
                .thenReturn(Flux.empty());
        when(batchBlobService.upload(
                any(), anyString(),
                anyString())).thenReturn(Mono.just(failedResponse));
        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectError().verify();

        BlobStorageException storageError =
                new BlobStorageException("upload failed", null, null);
        when(batchBlobService.upload(
                any(), anyString(),
                anyString())).thenReturn(Mono.error(storageError));
        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectErrorMatches(error -> error instanceof RuntimeException
                        && error.getCause() == storageError)
                .verify();
        verify(lifecyclePort, never()).saveBatch(any());
    }

    @Test
    void suspendAndRejectDelegateEachDecisionThenReturnTheFreshAggregate() {
        RewardBatch evaluating = batch("batch", RewardBatchStatus.EVALUATING);
        RewardBatch refreshed = batch("batch", RewardBatchStatus.EVALUATING);
        TransactionsRequest request = TransactionsRequest.builder()
                .transactionIds(List.of("one", "two")).reason("reason").build();
        when(lifecyclePort.findBatchWithStatus("batch", "initiative", RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(evaluating), Mono.just(evaluating));
        when(decisionPort.updateStatusAndReturnOld(
                eq("initiative"), eq("batch"),
                anyString(), any(),
                any(), eq(evaluating.getMonth()),
                isNull())).thenReturn(Mono.just(new RewardTransaction()));
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(refreshed));

        StepVerifier.create(service.suspendTransactions("batch", "initiative", request))
                .expectNext(refreshed).verifyComplete();
        StepVerifier.create(service.rejectTransactions("batch", "initiative", request))
                .expectNext(refreshed).verifyComplete();

        verify(auditUtilities).logTransactionsStatusChanged(
                eq(RewardBatchTrxStatus.SUSPENDED.name()),
                eq("initiative"), anyString(),
                isNull());
    }

    @Test
    void evaluationWithExplicitIdsSkipsMissingBatches() {
        RewardBatch sent = batch("sent", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatchWithStatus("sent", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(sent));
        when(lifecyclePort.findBatchWithStatus("missing", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.empty());
        when(decisionPort.prepareEvaluation("sent", "initiative")).thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(List.of("sent", "missing"), "initiative"))
                .expectNext(1L).verifyComplete();
    }

    @Test
    void evaluationWithNullOnlyListIsNormalizedToEmptyAndIsANoOp() {
        List<String> nullOnly = new ArrayList<>();
        nullOnly.add(null);

        StepVerifier.create(service.evaluatingRewardBatches(nullOnly, "initiative"))
                .expectNext(0L)
                .verifyComplete();

        verifyNoInteractions(lifecyclePort, decisionPort);
    }

    @Test
    void evaluationWithBlankOnlyListIsNormalizedToEmptyAndIsANoOp() {
        StepVerifier.create(service.evaluatingRewardBatches(List.of(" ", ""), "initiative"))
                .expectNext(0L)
                .verifyComplete();

        verifyNoInteractions(lifecyclePort, decisionPort);
    }

    @Test
    void evaluationWithMultipleTargetedIdsEvaluatesEachEligibleBatch() {
        RewardBatch b1 = batch("B1", RewardBatchStatus.SENT);
        RewardBatch b2 = batch("B2", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b1));
        when(lifecyclePort.findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b2));
        when(decisionPort.prepareEvaluation("B1", "initiative")).thenReturn(Mono.just(b1));
        when(decisionPort.prepareEvaluation("B2", "initiative")).thenReturn(Mono.just(b2));

        StepVerifier.create(service.evaluatingRewardBatches(List.of("B1", "B2"), "initiative"))
                .expectNext(2L)
                .verifyComplete();

        verify(lifecyclePort, never()).findBatchesWithStatus(any(), any());
        verify(decisionPort).prepareEvaluation("B1", "initiative");
        verify(decisionPort).prepareEvaluation("B2", "initiative");
    }

    @Test
    void evaluationWithMixedListProcessesOnlyNonNullNonBlankIds() {
        List<String> mixed = new ArrayList<>();
        mixed.add("B1");
        mixed.add(null);
        mixed.add("   ");
        mixed.add("B2");

        RewardBatch b1 = batch("B1", RewardBatchStatus.SENT);
        RewardBatch b2 = batch("B2", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b1));
        when(lifecyclePort.findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b2));
        when(decisionPort.prepareEvaluation("B1", "initiative")).thenReturn(Mono.just(b1));
        when(decisionPort.prepareEvaluation("B2", "initiative")).thenReturn(Mono.just(b2));

        StepVerifier.create(service.evaluatingRewardBatches(mixed, "initiative"))
                .expectNext(2L)
                .verifyComplete();

        verify(lifecyclePort, never()).findBatchesWithStatus(any(), any());
        verify(decisionPort).prepareEvaluation("B1", "initiative");
        verify(decisionPort).prepareEvaluation("B2", "initiative");
    }

    @Test
    void evaluationWithDuplicateIdsProcessesEachIdOnlyOnceInFirstOccurrenceOrder() {
        RewardBatch b2 = batch("B2", RewardBatchStatus.SENT);
        RewardBatch b1 = batch("B1", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b2));
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b1));
        when(decisionPort.prepareEvaluation("B2", "initiative")).thenReturn(Mono.just(b2));
        when(decisionPort.prepareEvaluation("B1", "initiative")).thenReturn(Mono.just(b1));

        StepVerifier.create(service.evaluatingRewardBatches(List.of("B2", "B1", "B2"), "initiative"))
                .expectNext(2L)
                .verifyComplete();

        verify(lifecyclePort, times(1)).findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT);
        verify(lifecyclePort, times(1)).findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT);
        verify(decisionPort, times(1)).prepareEvaluation("B2", "initiative");
        verify(decisionPort, times(1)).prepareEvaluation("B1", "initiative");

        var order = inOrder(decisionPort);
        order.verify(decisionPort).prepareEvaluation("B2", "initiative");
        order.verify(decisionPort).prepareEvaluation("B1", "initiative");
    }

    @Test
    void evaluationWithFullyIneligibleTargetedIdsIsANoOpWithoutQueryingAllBatchesOrDecisionPort() {
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.empty());
        when(lifecyclePort.findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.evaluatingRewardBatches(List.of("B1", "B2"), "initiative"))
                .expectNext(0L)
                .verifyComplete();

        verify(lifecyclePort, never()).findBatchesWithStatus(any(), any());
        verifyNoInteractions(decisionPort);
    }

    @Test
    void evaluationWhenPrepareEvaluationReturnsEmptyCountRemainsZero() {
        RewardBatch b1 = batch("B1", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b1));
        when(decisionPort.prepareEvaluation("B1", "initiative")).thenReturn(Mono.empty());

        StepVerifier.create(service.evaluatingRewardBatches(List.of("B1"), "initiative"))
                .expectNext(0L)
                .verifyComplete();

        verify(decisionPort).prepareEvaluation("B1", "initiative");
        verify(lifecyclePort, never()).findBatchesWithStatus(any(), any());
    }

    @Test
    void evaluationLookupErrorTerminatesProcessingAndNeverInvokesDecisionPort() {
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.error(new RuntimeException("lookup failure")));

        StepVerifier.create(service.evaluatingRewardBatches(List.of("B1", "B2"), "initiative"))
                .expectError(RuntimeException.class)
                .verify();

        verifyNoInteractions(decisionPort);
        verify(lifecyclePort, never()).findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT);
    }

    @Test
    void evaluationPrepareEvaluationErrorTerminatesProcessingAndEarlierBatchesAreCommitted() {
        RewardBatch b1 = batch("B1", RewardBatchStatus.SENT);
        RewardBatch b2 = batch("B2", RewardBatchStatus.SENT);
        when(lifecyclePort.findBatchWithStatus("B1", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b1));
        when(lifecyclePort.findBatchWithStatus("B2", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(b2));
        when(decisionPort.prepareEvaluation("B1", "initiative")).thenReturn(Mono.just(b1));
        when(decisionPort.prepareEvaluation("B2", "initiative"))
                .thenReturn(Mono.error(new RuntimeException("evaluation failure")));

        StepVerifier.create(service.evaluatingRewardBatches(List.of("B1", "B2", "B3"), "initiative"))
                .expectError(RuntimeException.class)
                .verify();

        // B1 was committed before B2 failed
        verify(decisionPort).prepareEvaluation("B1", "initiative");
        verify(decisionPort).prepareEvaluation("B2", "initiative");
        // B3 was never started because the stream terminated on B2's error
        verify(lifecyclePort, never()).findBatchWithStatus("B3", "initiative", RewardBatchStatus.SENT);
        verify(decisionPort, never()).prepareEvaluation("B3", "initiative");
    }

    @Test
    void batchOrchestratorsUsePaginatedPortsAndIsolatePerBatchFailures() {
        RewardBatch approving = batch("batch", RewardBatchStatus.APPROVING);
        RewardBatchServiceImpl worker = spy(service);
        doReturn(Mono.just(approving)).when(worker)
                .processSingleBatchConfirmation(approving, "initiative");
        when(lifecyclePort.findBatchesWithStatus(
                eq(RewardBatchStatus.APPROVING),
                eq("initiative"), any()))
                .thenReturn(Flux.just(approving), Flux.empty());

        StepVerifier.create(worker.rewardBatchConfirmationBatch("initiative", List.of()))
                .verifyComplete();
        verify(lifecyclePort, atLeastOnce()).findBatchesWithStatus(
                eq(RewardBatchStatus.APPROVING),
                eq("initiative"), any());
    }

    @Test
    void csvGenerationMapsTransactionRowsAndResolvesMissingFiscalCodes() {
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        RewardTransaction transaction = RewardTransaction.builder()
                .id("transaction").userId("user").trxCode("code")
                .trxChargeDate(java.time.LocalDateTime.parse("2026-01-01T12:30:00"))
                .effectiveAmountCents(1000L).fiscalCode(null).franchiseName("franchise")
                .additionalProperties(java.util.Map.of("productName", "product", "productGtin", "gtin"))
                .rewards(java.util.Map.of("initiative",
                        it.gov.pagopa.idpay.transactions.model.Reward.builder().accruedRewardCents(100L).build()))
                .invoiceData(it.gov.pagopa.idpay.transactions.dto.InvoiceData.builder()
                        .docNumber("document").filename("invoice.pdf").build())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).build();
        @SuppressWarnings("unchecked")
        com.azure.core.http.rest.Response<com.azure.storage.blob.models.BlockBlobItem> response =
                mock(com.azure.core.http.rest.Response.class);
        when(response.getStatusCode()).thenReturn(201);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        when(transactionReadPort.findBatchTransactions(
                "batch", "initiative", List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED)))
                .thenReturn(Flux.just(transaction));
        when(userRestClient.retrieveUserInfo("user")).thenReturn(Mono.just(
                it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV.builder().pii("fiscal-code").build()));
        when(batchBlobService.upload(
                any(), anyString(),
                anyString())).thenReturn(Mono.just(response));
        when(lifecyclePort.saveBatch(approved)).thenReturn(Mono.just(approved));

        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectNext("business_name_FISICO.csv").verifyComplete();

        assertEquals("fiscal-code", transaction.getFiscalCode());
    }

    @Test
    void deliveryWorkerValidatesEmptyAndAmbiguousInstitutionResponses() {
        RewardBatch requested = batch("batch", RewardBatchStatus.APPROVED);
        RewardBatch snapshotted = batch("batch", RewardBatchStatus.APPROVED);
        snapshotted.setDeliveryAmountCents(100L);
        MerchantDetailDTO merchant = MerchantDetailDTO.builder().fiscalCode("fiscal").build();
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(requested));
        when(deliveryPort.snapshotDeliveryAmount("batch", "initiative")).thenReturn(Mono.just(snapshotted));
        when(merchantRestClient.getMerchantDetail("merchant", "initiative")).thenReturn(Mono.just(merchant));
        when(selfcareClient.getInstitutions("fiscal"))
                .thenReturn(Mono.just(new InstitutionList(List.of())));
        StepVerifier.create(service.processSingleBatchDelivery(requested, "initiative"))
                .expectError().verify();

        InstitutionDTO institution = InstitutionDTO.builder().build();
        when(selfcareClient.getInstitutions("fiscal"))
                .thenReturn(Mono.just(new InstitutionList(List.of(institution, institution))));
        StepVerifier.create(service.processSingleBatchDelivery(requested, "initiative"))
                .expectError().verify();
    }

    @Test
    void outcomePollingUsesRequestedOrPendingRefundBatches() {
        RewardBatch pending = batch("batch", RewardBatchStatus.PENDING_REFUND);
        var completed = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("COMPLETATA").dateValue(LocalDate.parse("2026-01-02")).build()).build();
        when(lifecyclePort.findBatchWithStatus("batch", "initiative", RewardBatchStatus.PENDING_REFUND))
                .thenReturn(Mono.just(pending));
        when(erogazioniClient.getOutcome("batch")).thenReturn(Mono.just(completed));
        when(deliveryPort.recordRefundOutcome(
                "batch", "initiative", RewardBatchStatus.REFUNDED, LocalDate.parse("2026-01-02"), null))
                .thenReturn(Mono.just(batch("batch", RewardBatchStatus.REFUNDED)));
        StepVerifier.create(service.checkRewardBatchesOutcomes("initiative", List.of("batch")))
                .verifyComplete();

        when(lifecyclePort.findBatchesWithStatus(RewardBatchStatus.PENDING_REFUND, "initiative"))
                .thenReturn(Flux.empty());
        StepVerifier.create(service.checkRewardBatchesOutcomes("initiative", List.of()))
                .verifyComplete();
    }

    @Test
    void dateUtilitiesAndApprovalPreparationHandleValidAndInvalidPortStates() {
        assertEquals("2026-02", service.addOneMonth("2026-01"));
        assertEquals("febbraio 2026", service.addOneMonthToItalian("gennaio 2026"));
        assertEquals(YearMonth.now().toString(), service.getTargetMonth("2020-01"));
        assertEquals("2099-01", service.getTargetMonth("2099-01"));
        when(finalApprovalPort.prepareFinalApproval("batch", "initiative"))
                .thenReturn(Mono.empty());
        StepVerifier.create(service.updateAndSaveRewardTransactionsToApprove("batch", "initiative"))
                .expectError().verify();
    }

    @Test
    void sendRejectsWhenAnEarlierCreatedBatchStillHasTransactions() {
        RewardBatch current = batch("current", RewardBatchStatus.CREATED);
        current.setMonth(YearMonth.now().minusMonths(1).toString());
        RewardBatch earlier = batch("earlier", RewardBatchStatus.CREATED);
        earlier.setMonth(YearMonth.now().minusMonths(2).toString());
        when(lifecyclePort.findBatch("current")).thenReturn(Mono.just(current));
        when(lifecyclePort.findMerchantBatches("merchant", "initiative", PosType.PHYSICAL))
                .thenReturn(Flux.just(earlier));

        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "current"))
                .expectError().verify();
    }

    @Test
    void approvedFileOperatorScopeChecksRoleAndFilename() {
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        approved.setFilename(null);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, "operator1", "initiative", "batch"))
                .expectError().verify();

        approved.setFilename("file.csv");
        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, "admin", "initiative", "batch"))
                .expectError().verify();
    }

    @Test
    void confirmationRejectsPreviousNonRefundedBatch() {
        RewardBatch evaluating = batch("batch", RewardBatchStatus.EVALUATING);
        evaluating.setAssigneeLevel(RewardBatchAssignee.L3);
        RewardBatch previous = batch("previous", RewardBatchStatus.CREATED);
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(evaluating));
        when(listPort.findBatchesBeforeMonth("merchant", "initiative", PosType.PHYSICAL, evaluating.getMonth()))
                .thenReturn(Flux.just(previous));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectError().verify();
    }

    @Test
    void refundRejectionConcatenatesExternalErrorsAndUnknownStatusIsUnchanged() {
        RewardBatch pending = batch("batch", RewardBatchStatus.PENDING_REFUND);
        var rejected = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("RIFIUTATA").build())
                .errors(List.of(
                        new it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErrorInvitaliaDTO("a", "first"),
                        new it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErrorInvitaliaDTO("b", "second")))
                .build();
        when(deliveryPort.recordRefundOutcome(
                "batch", "initiative", RewardBatchStatus.NOT_REFUNDED, null, "a - first; b - second"))
                .thenReturn(Mono.just(batch("batch", RewardBatchStatus.NOT_REFUNDED)));
        StepVerifier.create(service.updateBatch(pending, rejected)).expectNextCount(1).verifyComplete();

        var unknown = it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO.builder()
                .erogazione(it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO.builder()
                        .status("UNKNOWN").build()).build();
        StepVerifier.create(service.updateBatch(pending, unknown)).expectNext(pending).verifyComplete();
    }

    @Test
    void deliveryBatchOrchestratorsUseExplicitAndPaginatedSelections() {
        RewardBatch batch = batch("batch", RewardBatchStatus.APPROVED);
        RewardBatchServiceImpl worker = spy(service);
        doReturn(Mono.just(batch)).when(worker)
                .processSingleBatchDelivery(batch, "initiative");
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(batch));
        StepVerifier.create(worker.rewardBatchDeliveryBatch("initiative", List.of("batch")))
                .verifyComplete();

        when(lifecyclePort.findDeliverableBatches(
                eq("initiative"), any()))
                .thenReturn(Flux.just(batch), Flux.empty());
        StepVerifier.create(worker.rewardBatchDeliveryBatch("initiative", List.of()))
                .verifyComplete();
    }

    @Test
    void confirmationWorkerKeepsApprovalWhenCsvGenerationFails() {
        RewardBatch approving = batch("batch", RewardBatchStatus.APPROVING);
        approving.setAssigneeLevel(RewardBatchAssignee.L3);
        RewardBatch prepared = batch("batch", RewardBatchStatus.APPROVING);
        prepared.setNumberOfTransactionsSuspended(0L);
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        RewardBatchServiceImpl worker = spy(service);
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(approving));
        when(finalApprovalPort.prepareFinalApproval("batch", "initiative")).thenReturn(Mono.just(prepared));
        when(finalApprovalPort.completeFinalApproval("batch", "initiative")).thenReturn(Mono.just(approved));
        doReturn(Mono.error(new IllegalStateException("storage"))).when(worker)
                .generateAndSaveCsv("batch", "initiative", "merchant");

        StepVerifier.create(worker.processSingleBatchConfirmation(approving, "initiative"))
                .expectNext(approved).verifyComplete();
    }

    private static RewardBatch batch(String id, RewardBatchStatus status) {
        return RewardBatch.builder()
                .id(id).initiativeId("initiative").merchantId("merchant")
                .month(YearMonth.now().minusMonths(1).toString()).posType(PosType.PHYSICAL)
                .status(status).name("name").businessName("business")
                .numberOfTransactions(1L).build();
    }
}
