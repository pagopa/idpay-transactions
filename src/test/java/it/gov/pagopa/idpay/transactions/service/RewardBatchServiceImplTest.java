package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.PaymentRestClient;
import it.gov.pagopa.idpay.transactions.data.factory.DataFactoryService;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.exception.AzureConnectingErrorException;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.*;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.*;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
    @Mock private ApprovedRewardBatchBlobService batchBlobService;
    @Mock private ChecksErrorMapper checksErrorMapper;
    @Mock private AuditUtilities auditUtilities;
    @Mock private MerchantRestClient merchantRestClient;
    @Mock private PaymentRestClient paymentRestClient;
    @Mock private SelfcareInstitutionsRestClient selfcareClient;
    @Mock private ErogazioniRestClient erogazioniClient;
    @Mock private InitiativeDataService initiativeDataService;
    @Mock private DataFactoryService dataFactoryService;
    @Mock private ReportPersistencePort reportPersistencePort;
    private RewardBatchServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new RewardBatchServiceImpl(
                lifecyclePort, listPort, merchantLookupPort, transactionReadPort, decisionPort,
                postponementPort, finalApprovalPort, promotionPort, deliveryPort, reassignmentPort,
                batchBlobService, checksErrorMapper, auditUtilities, merchantRestClient,
                paymentRestClient, selfcareClient, erogazioniClient, initiativeDataService,
                dataFactoryService, reportPersistencePort, 10);
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
        when(transactionReadPort.findBatchTransactionIds(eq("batch"), eq("initiative"), anyInt(), anyInt()))
                .thenReturn(Flux.just("trx-1", "trx-2"), Flux.empty());
        when(paymentRestClient.updateTransactionsStatus(java.util.Set.of("trx-1", "trx-2"), SyncTrxStatus.REWARDED))
                .thenReturn(Mono.just(2));
        when(decisionPort.prepareEvaluation("batch", "initiative")).thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(null, "initiative"))
                .expectNext(1L)
                .verifyComplete();

        verify(lifecyclePort).findBatchesWithStatus(RewardBatchStatus.SENT, "initiative");
        verify(lifecyclePort, never()).findBatchWithStatus(
                anyString(), eq("initiative"), eq(RewardBatchStatus.SENT));
        InOrder inOrder = inOrder(paymentRestClient, decisionPort);
        inOrder.verify(paymentRestClient)
                .updateTransactionsStatus(java.util.Set.of("trx-1", "trx-2"), SyncTrxStatus.REWARDED);
        inOrder.verify(decisionPort).prepareEvaluation("batch", "initiative");
    }

    @Test
    void evaluationStopsBeforeSqlPreparationWhenPaymentStatusSyncFails() {
        RewardBatch sent = RewardBatch.builder().id("batch").build();
        RuntimeException failure = new IllegalStateException("payment status update failed");
        when(lifecyclePort.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative"))
                .thenReturn(Flux.just(sent));
        when(transactionReadPort.findBatchTransactionIds(eq("batch"), eq("initiative"), anyInt(), anyInt()))
                .thenReturn(Flux.just("trx-1"));
        when(paymentRestClient.updateTransactionsStatus(Set.of("trx-1"), SyncTrxStatus.REWARDED))
                .thenReturn(Mono.error(failure));

        StepVerifier.create(service.evaluatingRewardBatches(null, "initiative"))
                .expectErrorMatches(error -> error == failure)
                .verify();

        verify(decisionPort, never()).prepareEvaluation("batch", "initiative");
    }

    @Test
    void evaluationSplitsPaymentStatusUpdateIntoBatchesOfAtMost100Ids() {
        RewardBatch sent = RewardBatch.builder().id("batch").build();
        List<String> transactionIds = IntStream.rangeClosed(1, 205)
                .mapToObj(i -> "trx-" + i)
                .toList();

        when(lifecyclePort.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative"))
                .thenReturn(Flux.just(sent));
        when(transactionReadPort.findBatchTransactionIds(eq("batch"), eq("initiative"), anyInt(), anyInt()))
                .thenReturn(Flux.fromIterable(transactionIds), Flux.empty());
        when(paymentRestClient.updateTransactionsStatus(any(), eq(SyncTrxStatus.REWARDED)))
                .thenAnswer(invocation -> {
                    Set<String> chunk = invocation.getArgument(0);
                    if (chunk.size() > 100) {
                        return Mono.error(new IllegalArgumentException("Chunk size exceeds 100"));
                    }
                    return Mono.just(chunk.size());
                });
        when(decisionPort.prepareEvaluation("batch", "initiative")).thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(null, "initiative"))
                .expectNext(1L)
                .verifyComplete();

        verify(paymentRestClient, times(3)).updateTransactionsStatus(any(), eq(SyncTrxStatus.REWARDED));
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
    void sendBatchDelegatesTheWholeTransitionToTheLifecyclePort() {
        RewardBatch sent = batch("batch", RewardBatchStatus.SENT);
        when(lifecyclePort.sendBatch("batch", "initiative", "merchant"))
                .thenReturn(Mono.just(sent));

        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "batch"))
                .verifyComplete();

        verify(lifecyclePort).sendBatch("batch", "initiative", "merchant");
        verifyNoMoreInteractions(lifecyclePort);
    }

    @Test
    void sendBatchPropagatesLifecyclePortFailure() {
        IllegalStateException failure = new IllegalStateException("atomic send failed");
        when(lifecyclePort.sendBatch("batch", "initiative", "merchant"))
                .thenReturn(Mono.error(failure));

        StepVerifier.create(service.sendRewardBatch("initiative", "merchant", "batch"))
                .expectErrorMatches(error -> error == failure)
                .verify();

        verify(lifecyclePort).sendBatch("batch", "initiative", "merchant");
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
        approved.setReportId("report-approved");
        when(merchantLookupPort.findMerchantBatch("merchant", "initiative", "approved"))
                .thenReturn(Mono.just(approved));
        when(reportPersistencePort.findByIdAndInitiativeId("report-approved", "initiative"))
                .thenReturn(Mono.just(Report.builder()
                        .id("report-approved")
                        .initiativeId("initiative")
                        .fileName("approved.csv")
                        .reportStatus(ReportStatus.GENERATED)
                        .reportType(ReportType.REWARD_BATCH_CSV)
                        .build()));
        when(batchBlobService.getFileSignedUrl(
                "initiative/initiative/merchant/merchant/batch/approved/approved.csv"))
                .thenReturn(Mono.just("signed-url"));
        StepVerifier.create(service.downloadApprovedRewardBatchFile(
                        "merchant", null, "initiative", "approved"))
                .assertNext(response -> assertEquals("signed-url", response.getApprovedBatchUrl()))
                .verifyComplete();
    }

    @Test
    void confirmationDelegatesAtomicApprovalEntryToTheLifecyclePort() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(10L);
        existingBatch.setNumberOfTransactionsElaborated(2L);

        RewardBatch approving = batch("batch", RewardBatchStatus.APPROVING);

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));
        when(lifecyclePort.enterApproval("batch", "initiative")).thenReturn(Mono.just(approving));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .assertNext(result -> assertEquals(RewardBatchStatus.APPROVING, result.getStatus()))
                .verifyComplete();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort).enterApproval("batch", "initiative");
        verify(lifecyclePort, never()).saveBatch(any());
        verifyNoInteractions(listPort);
    }

    @Test
    void confirmationPropagatesAtomicApprovalEntryFailure() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(10L);
        existingBatch.setNumberOfTransactionsElaborated(2L);

        IllegalStateException failure = new IllegalStateException("approval entry failed");

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));
        when(lifecyclePort.enterApproval("batch", "initiative")).thenReturn(Mono.error(failure));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectErrorMatches(error -> error == failure)
                .verify();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort).enterApproval("batch", "initiative");
        verifyNoInteractions(listPort);
    }

    @Test
    void confirmationFailsWhenElaboratedTransactionsBelowThreshold() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(10L);
        existingBatch.setNumberOfTransactionsElaborated(1L);

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectError()
                .verify();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort, never()).enterApproval(anyString(), anyString());
        verifyNoInteractions(listPort);
    }

    @Test
    void confirmationFailsWhenBatchNotFound() {
        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.empty());

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectError()
                .verify();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort, never()).enterApproval(anyString(), anyString());
        verifyNoInteractions(listPort);
    }

    @Test
    void confirmationSucceedsWhenNumberOfTransactionsIsNull() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(null);
        existingBatch.setNumberOfTransactionsElaborated(5L);

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));
        when(lifecyclePort.enterApproval("batch", "initiative")).thenReturn(Mono.just(existingBatch));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectNext(existingBatch)
                .verifyComplete();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort).enterApproval("batch", "initiative");
    }

    @Test
    void confirmationFailsWhenNumberOfTransactionsElaboratedIsNull() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(10L);
        existingBatch.setNumberOfTransactionsElaborated(null);

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectError()
                .verify();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort, never()).enterApproval(anyString(), anyString());
    }

    @Test
    void confirmationSucceedsWhenTotalTransactionsIsZero() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(0L);
        existingBatch.setNumberOfTransactionsElaborated(0L);

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));
        when(lifecyclePort.enterApproval("batch", "initiative")).thenReturn(Mono.just(existingBatch));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectNext(existingBatch)
                .verifyComplete();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort).enterApproval("batch", "initiative");
    }

    @Test
    void confirmationFailsWhenTotalTransactionsIsNegative() {
        RewardBatch existingBatch = batch("batch", RewardBatchStatus.EVALUATING);
        existingBatch.setNumberOfTransactions(-1L);
        existingBatch.setNumberOfTransactionsElaborated(0L);

        when(lifecyclePort.findBatch("batch", "initiative")).thenReturn(Mono.just(existingBatch));

        StepVerifier.create(service.rewardBatchConfirmation("initiative", "batch"))
                .expectError()
                .verify();

        verify(lifecyclePort).findBatch("batch", "initiative");
        verify(lifecyclePort, never()).enterApproval(anyString(), anyString());
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
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        when(lifecyclePort.saveBatch(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(reportPersistencePort.save(any(Report.class))).thenReturn(Mono.just(Report.builder()
                .id("report-123").initiativeId("initiative").merchantId("merchant")
                .reportType(ReportType.REWARD_BATCH_CSV).reportStatus(ReportStatus.INSERTED).fileName("business_name_FISICO.csv").build()));
        when(dataFactoryService.triggerRewardBatchCsvPipeline(
                "initiative", "merchant", "batch", "report-123", "business_name_FISICO.csv"
        )).thenReturn(Mono.just("run-123"));


        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectNext("business_name_FISICO.csv").verifyComplete();

        verify(reportPersistencePort).save(argThat(report ->
                ReportType.REWARD_BATCH_CSV.equals(report.getReportType())
                        && "business_name_FISICO.csv".equals(report.getFileName())
                        && ReportStatus.INSERTED.equals(report.getReportStatus())
                        && "business".equals(report.getBusinessName())
                        && RewardBatchAssignee.L3.equals(report.getOperatorLevel())
                        && approved.getStartDate().equals(report.getStartPeriod())
                        && approved.getEndDate().equals(report.getEndPeriod())
                        && report.getRequestDate() != null));
        verify(lifecyclePort).saveBatch(argThat(batch -> "report-123".equals(batch.getReportId())));
    }

    @Test
    void csvGenerationSurfacesStorageFailureAndMissingBatch() {
        when(lifecyclePort.findBatch("missing")).thenReturn(Mono.empty());
        StepVerifier.create(service.generateAndSaveCsv("missing", "initiative", "merchant"))
                .expectError().verify();

        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        when(lifecyclePort.saveBatch(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(reportPersistencePort.save(any(Report.class))).thenAnswer(invocation -> {
            Report report = invocation.getArgument(0);
            if (report.getId() == null) {
                report.setId("report-123");
            }
            return Mono.just(report);
        });
        when(reportPersistencePort.findByIdAndInitiativeId(anyString(), anyString())).thenReturn(Mono.just(Report.builder()
                .id("report-123").initiativeId("initiative").merchantId("merchant")
                .reportType(ReportType.REWARD_BATCH_CSV).reportStatus(ReportStatus.INSERTED).fileName("business_name_FISICO.csv").build()));
        when(dataFactoryService.triggerRewardBatchCsvPipeline(
                "initiative", "merchant", "batch", "report-123", "business_name_FISICO.csv"
        )).thenReturn(Mono.error(new AzureConnectingErrorException("adf", new RuntimeException("down"))));

        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectError().verify();

        verify(reportPersistencePort, atLeastOnce()).save(argThat(report ->
                ReportStatus.FAILED.equals(report.getReportStatus())
                        && ReportType.REWARD_BATCH_CSV.equals(report.getReportType())));
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
        when(transactionReadPort.findBatchTransactionIds(eq("sent"), eq("initiative"), anyInt(), anyInt()))
                .thenReturn(Flux.just("trx-1"), Flux.empty());
        when(paymentRestClient.updateTransactionsStatus(java.util.Set.of("trx-1"), SyncTrxStatus.REWARDED))
                .thenReturn(Mono.just(1));
        when(decisionPort.prepareEvaluation("sent", "initiative")).thenReturn(Mono.just(sent));

        StepVerifier.create(service.evaluatingRewardBatches(List.of("sent", "missing"), "initiative"))
                .expectNext(1L).verifyComplete();
    }

    @Test
    void batchOrchestratorsUsePaginatedPortsAndIsolatePerBatchFailures() {
        RewardBatch approving = batch("batch", RewardBatchStatus.APPROVING);
        RewardBatchServiceImpl worker = spy(service);
        doReturn(Mono.error(new IllegalStateException("Confirmation failed"))).when(worker)
                .processSingleBatchConfirmation(approving, "initiative");
        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVING, "initiative", null, 10))
                .thenReturn(Flux.just(approving));
        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVING, "initiative", "batch", 10))
                .thenReturn(Flux.empty());

        StepVerifier.create(worker.rewardBatchConfirmationBatch("initiative", List.of()))
                .expectComplete().verify(java.time.Duration.ofSeconds(3));
        verify(worker).processSingleBatchConfirmation(approving, "initiative");
        verify(lifecyclePort).findBatchesToProcessAfter(RewardBatchStatus.APPROVING, "initiative", "batch", 10);
    }

    @Test
    void csvGenerationMapsTransactionRowsAndResolvesMissingFiscalCodes() {
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        lenient().when(lifecyclePort.saveBatch(any(RewardBatch.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        lenient().when(reportPersistencePort.save(any(Report.class))).thenReturn(Mono.just(Report.builder()
                .id("report-123").initiativeId("initiative").merchantId("merchant")
                .reportType(ReportType.REWARD_BATCH_CSV).reportStatus(ReportStatus.INSERTED).fileName("business_name_FISICO.csv").build()));

        StepVerifier.create(service.generateAndSaveCsv("batch", "initiative", "merchant"))
                .expectNext("business_name_FISICO.csv").verifyComplete();
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
    void approvedFileOperatorScopeChecksRoleAndFilename() {
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        when(lifecyclePort.findBatch("batch")).thenReturn(Mono.just(approved));
        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, "operator1", "initiative", "batch"))
                .expectError().verify();

        approved.setReportId("report-1");
        StepVerifier.create(service.downloadApprovedRewardBatchFile(null, "admin", "initiative", "batch"))
                .expectError().verify();
    }

    @Test
    void approvedFileRequiresGeneratedCsvStatusWhenPresent() {
        RewardBatch approved = batch("batch", RewardBatchStatus.APPROVED);
        approved.setReportId("report-1");
        when(merchantLookupPort.findMerchantBatch("merchant", "initiative", "batch"))
                .thenReturn(Mono.just(approved));
        when(reportPersistencePort.findByIdAndInitiativeId("report-1", "initiative"))
                .thenReturn(Mono.just(Report.builder()
                        .id("report-1")
                        .initiativeId("initiative")
                        .fileName("file.csv")
                        .reportStatus(ReportStatus.IN_PROGRESS)
                        .reportType(ReportType.REWARD_BATCH_CSV)
                        .build()));

        StepVerifier.create(service.downloadApprovedRewardBatchFile("merchant", null, "initiative", "batch"))
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

        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVED, "initiative", null, 10))
                .thenReturn(Flux.just(batch));
        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVED, "initiative", "batch", 10))
                .thenReturn(Flux.empty());
        StepVerifier.create(worker.rewardBatchDeliveryBatch("initiative", List.of()))
                .verifyComplete();
    }

    @Test
    void deliveryWithoutInstitutionsAdvancesToNextPageAndCanBeRetriedOnNextRun() {
        RewardBatch missing = batch("a", RewardBatchStatus.APPROVED);
        RewardBatch rejected = batch("b", RewardBatchStatus.APPROVED);
        RewardBatch accepted = batch("c", RewardBatchStatus.APPROVED);
        RewardBatchServiceImpl worker = spy(service);
        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVED, "initiative", null, 10))
                .thenReturn(Flux.just(missing));
        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVED, "initiative", "a", 10))
                .thenReturn(Flux.just(rejected, accepted));
        when(lifecyclePort.findBatchesToProcessAfter(RewardBatchStatus.APPROVED, "initiative", "c", 10))
                .thenReturn(Flux.empty());
        when(lifecyclePort.findBatch("a", "initiative")).thenReturn(Mono.just(missing));
        when(deliveryPort.snapshotDeliveryAmount("a", "initiative")).thenReturn(Mono.just(missing));
        when(merchantRestClient.getMerchantDetail("merchant", "initiative"))
                .thenReturn(Mono.just(MerchantDetailDTO.builder().fiscalCode("fiscal").build()));
        when(selfcareClient.getInstitutions("fiscal"))
                .thenReturn(Mono.just(new InstitutionList(List.of())));
        org.mockito.Mockito.doCallRealMethod().when(worker).processSingleBatchDelivery(missing, "initiative");
        doReturn(Mono.just(rejected)).when(worker).processSingleBatchDelivery(rejected, "initiative");
        doReturn(Mono.fromSupplier(() -> {
            accepted.setStatus(RewardBatchStatus.PENDING_REFUND);
            return accepted;
        })).when(worker).processSingleBatchDelivery(accepted, "initiative");

        Mono<Void> execution = worker.rewardBatchDeliveryBatch("initiative", List.of());
        StepVerifier.create(execution).expectComplete().verify(java.time.Duration.ofSeconds(3));
        verify(selfcareClient).getInstitutions("fiscal");
        verify(worker).processSingleBatchDelivery(rejected, "initiative");
        verify(worker).processSingleBatchDelivery(accepted, "initiative");
        assertEquals(RewardBatchStatus.APPROVED, missing.getStatus());
        verifyNoInteractions(erogazioniClient);

        StepVerifier.create(execution).expectComplete().verify(java.time.Duration.ofSeconds(3));
        verify(selfcareClient, times(2)).getInstitutions("fiscal");
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
                .startDate(java.time.LocalDateTime.parse("2026-01-01T00:00:00"))
                .endDate(java.time.LocalDateTime.parse("2026-01-31T23:59:59"))
                .approvalDate(java.time.LocalDateTime.parse("2026-02-01T10:00:00"))
                .numberOfTransactions(1L).build();
    }

    @Test
    void generateAndSaveCsvPersistsReportAndLinksBatchToReportId() {
        RewardBatch batch = batch("batch-1", RewardBatchStatus.APPROVED);
        batch.setBusinessName("Merchant");
        batch.setName("reward-batch");
        batch.setPosType(PosType.PHYSICAL);
        batch.setStartDate(LocalDateTime.of(2026, 7, 1, 0, 0));
        batch.setEndDate(LocalDateTime.of(2026, 7, 31, 23, 59, 59));
        Report report = Report.builder()
                .id("report-123")
                .initiativeId("initiative")
                .merchantId("merchant")
                .reportType(ReportType.REWARD_BATCH_CSV)
                .reportStatus(ReportStatus.INSERTED)
                .build();

        when(lifecyclePort.findBatch("batch-1")).thenReturn(Mono.just(batch));
        when(reportPersistencePort.save(any(Report.class))).thenReturn(Mono.just(report));
        when(lifecyclePort.saveBatch(any(RewardBatch.class))).thenAnswer(invocation -> {
            RewardBatch saved = invocation.getArgument(0);
            saved.setReportId("report-123");
            return Mono.just(saved);
        });
        when(dataFactoryService.triggerRewardBatchCsvPipeline(
                "initiative", "merchant", "batch-1", "report-123", "Merchant_reward-batch_FISICO.csv"
        )).thenReturn(Mono.just("run-123"));

        StepVerifier.create(service.generateAndSaveCsv("batch-1", "initiative", "merchant"))
                .expectNext("Merchant_reward-batch_FISICO.csv")
                .verifyComplete();

        verify(reportPersistencePort).save(argThat(saved ->
                ReportType.REWARD_BATCH_CSV.equals(saved.getReportType())
                        && "Merchant_reward-batch_FISICO.csv".equals(saved.getFileName())
                        && "Merchant".equals(saved.getBusinessName())
                        && batch.getStartDate().equals(saved.getStartPeriod())
                        && batch.getEndDate().equals(saved.getEndPeriod())
                        && RewardBatchAssignee.L3.equals(saved.getOperatorLevel())
                        && saved.getRequestDate() != null));
        verify(lifecyclePort).saveBatch(argThat(updated -> "report-123".equals(updated.getReportId())));
    }

    @Test
    void generateAndSaveCsvReusesExistingReportForSameBatch() {
        RewardBatch batch = batch("batch-2", RewardBatchStatus.APPROVED);
        batch.setBusinessName("Merchant");
        batch.setName("reward-batch");
        batch.setPosType(PosType.PHYSICAL);
        batch.setReportId("report-existing");
        Report existing = Report.builder()
                .id("report-existing")
                .initiativeId("initiative")
                .merchantId("merchant")
                .reportType(ReportType.REWARD_BATCH_CSV)
                .reportStatus(ReportStatus.INSERTED)
                .fileName("Merchant_reward-batch_FISICO.csv")
                .build();

        when(lifecyclePort.findBatch("batch-2")).thenReturn(Mono.just(batch));
        when(reportPersistencePort.findByIdAndInitiativeId("report-existing", "initiative"))
                .thenReturn(Mono.just(existing));
        when(reportPersistencePort.save(any(Report.class))).thenReturn(Mono.just(existing));
        when(dataFactoryService.triggerRewardBatchCsvPipeline(
                "initiative", "merchant", "batch-2", "report-existing", "Merchant_reward-batch_FISICO.csv"
        )).thenReturn(Mono.just("run-456"));

        StepVerifier.create(service.generateAndSaveCsv("batch-2", "initiative", "merchant"))
                .expectNext("Merchant_reward-batch_FISICO.csv")
                .verifyComplete();

        verify(reportPersistencePort, atLeastOnce()).save(argThat(saved ->
                "report-existing".equals(saved.getId())
                        && ReportType.REWARD_BATCH_CSV.equals(saved.getReportType())));
        verify(lifecyclePort, never()).saveBatch(any(RewardBatch.class));
    }

    @Test
    void generateAndSaveCsvWhenDataFactoryFailsMarksReportAsFailed() {
        RewardBatch batch = batch("batch-3", RewardBatchStatus.APPROVED);
        batch.setBusinessName("Merchant");
        batch.setName("reward-batch");
        batch.setPosType(PosType.PHYSICAL);
        Report persisted = Report.builder()
                .id("report-999")
                .initiativeId("initiative")
                .merchantId("merchant")
                .reportType(ReportType.REWARD_BATCH_CSV)
                .reportStatus(ReportStatus.INSERTED)
                .fileName("Merchant_reward-batch_FISICO.csv")
                .build();

        when(lifecyclePort.findBatch("batch-3")).thenReturn(Mono.just(batch));
        when(reportPersistencePort.save(any(Report.class))).thenReturn(Mono.just(persisted));
        when(reportPersistencePort.findByIdAndInitiativeId("report-999", "initiative"))
                .thenReturn(Mono.just(persisted));
        when(lifecyclePort.saveBatch(any(RewardBatch.class))).thenAnswer(invocation -> {
            RewardBatch saved = invocation.getArgument(0);
            saved.setReportId("report-999");
            return Mono.just(saved);
        });
        when(dataFactoryService.triggerRewardBatchCsvPipeline(
                "initiative", "merchant", "batch-3", "report-999", "Merchant_reward-batch_FISICO.csv"
        )).thenReturn(Mono.error(new AzureConnectingErrorException("adf", new RuntimeException("down"))));

        StepVerifier.create(service.generateAndSaveCsv("batch-3", "initiative", "merchant"))
                .expectError(AzureConnectingErrorException.class)
                .verify();

        verify(reportPersistencePort, atLeastOnce()).save(argThat(saved ->
                ReportStatus.FAILED.equals(saved.getReportStatus())
                        && ReportType.REWARD_BATCH_CSV.equals(saved.getReportType())
                        && "report-999".equals(saved.getId())));
        verify(lifecyclePort, atLeastOnce()).saveBatch(argThat(updated -> "report-999".equals(updated.getReportId())));
    }

    @Test
    void approvedFileRejectsMissingFilenameBeforeBlobLookup() {
        RewardBatch approved = batch("batch-missing-name", RewardBatchStatus.APPROVED);
        approved.setReportId("report-missing-name");
        when(merchantLookupPort.findMerchantBatch("merchant", "initiative", "batch-missing-name"))
                .thenReturn(Mono.just(approved));
        when(reportPersistencePort.findByIdAndInitiativeId("report-missing-name", "initiative"))
                .thenReturn(Mono.just(Report.builder()
                        .id("report-missing-name")
                        .initiativeId("initiative")
                        .reportStatus(ReportStatus.GENERATED)
                        .reportType(ReportType.REWARD_BATCH_CSV)
                        .build()));

        StepVerifier.create(service.downloadApprovedRewardBatchFile("merchant", null, "initiative", "batch-missing-name"))
                .expectError(ClientExceptionWithBody.class)
                .verify();

        verifyNoInteractions(batchBlobService);
    }
}
