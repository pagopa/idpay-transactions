package it.gov.pagopa.idpay.transactions.service;

import com.azure.storage.blob.models.BlobStorageException;
import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.*;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantTransactionPostponementPort;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantRewardBatchLookupPort;
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
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {

    private static final String DATE_FORMAT = "yyyy-MM";
    private final RewardBatchLifecyclePort rewardBatchLifecyclePort;
    private final RewardBatchListPort rewardBatchListPort;
    private final MerchantRewardBatchLookupPort merchantRewardBatchLookupPort;
    private final RewardBatchTransactionReadPort rewardBatchTransactionReadPort;
    private final RewardBatchTransactionDecisionPort rewardBatchTransactionDecisionPort;
    private final MerchantTransactionPostponementPort merchantTransactionPostponementPort;
    private final RewardBatchFinalApprovalPort rewardBatchFinalApprovalPort;
    private final RewardBatchAssigneePromotionPort rewardBatchAssigneePromotionPort;
    private final RewardBatchDeliveryPort rewardBatchDeliveryPort;
    private final SuspendedTransactionReassignmentPort suspendedTransactionReassignmentPort;
    private final UserRestClient userRestClient;
    private final ChecksErrorMapper checksErrorMapper;

    private final AuditUtilities auditUtilities;
    private final MerchantRestClient merchantRestClient;
    private final SelfcareInstitutionsRestClient selfcareInstitutionsRestClient;
    private final ErogazioniRestClient erogazioniRestClient;
    private final InitiativeDataService initiativeDataService;

    private final int pagesize;


    private static final String OPERATOR_1 = "operator1";
    private static final String OPERATOR_2 = "operator2";
    private static final String OPERATOR_3 = "operator3";

    private static final Set<String> OPERATORS = Set.of(OPERATOR_1, OPERATOR_2, OPERATOR_3);
    private final ApprovedRewardBatchBlobService approvedRewardBatchBlobService;

    private static final String CSV_HEADER = String.join(";",
            "Data e ora",
            "Elettrodomestico",
            "Codice Fiscale Beneficiario",
            "ID transazione",
            "Codice sconto",
            "Totale della spesa",
            "Sconto applicato", // "Importo autorizzato",
            "Numero fattura",
            "Fattura",
            "Stato",
            "Punto vendita"
    );

    private static final String REWARD_BATCHES_PATH_STORAGE_FORMAT = "initiative/%s/merchant/%s/batch/%s/";
    private static final String REWARD_BATCHES_REPORT_NAME_FORMAT = "%s_%s_%s.csv";
    private static final String PROCESSING_BATCH_LOG = "Processing batch {}";
    private static final String FAILED_TO_PROCESS_BATCH_LOG = "Failed to process batch {}: {}";

    public RewardBatchServiceImpl(RewardBatchLifecyclePort rewardBatchLifecyclePort,
                                  RewardBatchListPort rewardBatchListPort,
                                  MerchantRewardBatchLookupPort merchantRewardBatchLookupPort,
                                  RewardBatchTransactionReadPort rewardBatchTransactionReadPort,
                                  RewardBatchTransactionDecisionPort rewardBatchTransactionDecisionPort,
                                  MerchantTransactionPostponementPort merchantTransactionPostponementPort,
                                  RewardBatchFinalApprovalPort rewardBatchFinalApprovalPort,
                                  RewardBatchAssigneePromotionPort rewardBatchAssigneePromotionPort,
                                  RewardBatchDeliveryPort rewardBatchDeliveryPort,
                                  SuspendedTransactionReassignmentPort suspendedTransactionReassignmentPort,
                                  UserRestClient userRestClient,
                                  ApprovedRewardBatchBlobService approvedRewardBatchBlobService,
                                  ChecksErrorMapper checksErrorMapper,
                                  AuditUtilities auditUtilities,
                                  MerchantRestClient merchantRestClient,
                                  SelfcareInstitutionsRestClient selfcareInstitutionsRestClient,
                                  ErogazioniRestClient erogazioniRestClient,
                                  InitiativeDataService initiativeDataService,
                                  @Value("${app.batch.paginationSize}") int pagesize) {
        this.rewardBatchLifecyclePort = rewardBatchLifecyclePort;
        this.rewardBatchListPort = rewardBatchListPort;
        this.merchantRewardBatchLookupPort = merchantRewardBatchLookupPort;
        this.rewardBatchTransactionReadPort = rewardBatchTransactionReadPort;
        this.rewardBatchTransactionDecisionPort = rewardBatchTransactionDecisionPort;
        this.merchantTransactionPostponementPort = merchantTransactionPostponementPort;
        this.rewardBatchFinalApprovalPort = rewardBatchFinalApprovalPort;
        this.rewardBatchAssigneePromotionPort = rewardBatchAssigneePromotionPort;
        this.rewardBatchDeliveryPort = rewardBatchDeliveryPort;
        this.suspendedTransactionReassignmentPort = suspendedTransactionReassignmentPort;
        this.userRestClient = userRestClient;
        this.approvedRewardBatchBlobService = approvedRewardBatchBlobService;
        this.checksErrorMapper = checksErrorMapper;
        this.auditUtilities = auditUtilities;
        this.merchantRestClient = merchantRestClient;
        this.selfcareInstitutionsRestClient = selfcareInstitutionsRestClient;
        this.erogazioniRestClient = erogazioniRestClient;
        this.initiativeDataService = initiativeDataService;
        this.pagesize = pagesize;
    }

    @Override
    public Mono<Page<RewardBatch>> getRewardBatches(String merchantId, String initiativeId, String organizationRole, String status, String assigneeLevel, String month, Pageable pageable) {
        boolean callerIsOperator = isOperator(organizationRole);

        return rewardBatchListPort.findRewardBatches(merchantId, initiativeId, status, assigneeLevel, month, callerIsOperator, pageable)
                .collectList()
                .zipWith(rewardBatchListPort.countRewardBatches(merchantId, initiativeId, status, assigneeLevel, month, callerIsOperator))
                .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
    }

    private boolean isOperator(String role) {
        return role != null && OPERATORS.contains(role.toLowerCase());
    }

    @Override
    public Mono<Void> sendRewardBatch(String initiativeId, String merchantId, String batchId) {
        return rewardBatchLifecyclePort.findBatch(batchId)
                .switchIfEmpty(Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND)))
                .flatMap(batch -> {
                    if (!merchantId.equals(batch.getMerchantId())) {
                        log.warn("[SEND_REWARD_BATCHES] Merchant id mismatch !");
                        return Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
                                ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND));
                    }
                    if (batch.getStatus() != RewardBatchStatus.CREATED) {
                        return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                                ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST));
                    }
                    YearMonth batchMonth = YearMonth.parse(batch.getMonth());
                    if (!YearMonth.now(ZONEID).isAfter(batchMonth)) {
                        log.warn("[SEND_REWARD_BATCHES] Batch month too early to be sent !");
                        return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                                ExceptionConstants.ExceptionCode.REWARD_BATCH_MONTH_TOO_EARLY));
                    }

                    return anyPreviousBatchesInCreatedStatusNotEmpty(initiativeId, merchantId, batchMonth, batch.getPosType())
                            .flatMap(allPreviousSent -> {
                                if (Boolean.TRUE.equals(allPreviousSent)) {
                                    log.warn("[SEND_REWARD_BATCHES] Previous batches of type {} not sent yet for merchant {}!",
                                            batch.getPosType(), Utilities.sanitizeString(merchantId));
                                    return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                                            ExceptionConstants.ExceptionCode.REWARD_BATCH_PREVIOUS_NOT_SENT));
                                }

                                LocalDateTime dateTimeNow = LocalDateTime.now(ZONEID);
                                batch.setStatus(RewardBatchStatus.SENT);
                                batch.setMerchantSendDate(dateTimeNow);
                                batch.setUpdateDate(dateTimeNow);
                                return rewardBatchLifecyclePort.saveBatch(batch);
                            })
                            .then();
                });
    }

    private Mono<Boolean> anyPreviousBatchesInCreatedStatusNotEmpty(String initiativeId, String merchantId, YearMonth currentMonth, PosType posType) {
        return rewardBatchLifecyclePort.findMerchantBatches(merchantId, initiativeId, posType)
                .filter(batch -> {
                    YearMonth batchMonth = YearMonth.parse(batch.getMonth());
                    return batchMonth.isBefore(currentMonth);
                })
                .filter(batch -> batch.getStatus() == RewardBatchStatus.CREATED)
                .filter(batch -> batch.getNumberOfTransactions() != 0)
                .hasElements();
    }

    @Override
    public Mono<RewardBatch> suspendTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request) {
        validChecksError(request.getChecksError());
        return updateTransactionStatuses(
                rewardBatchId,
                initiativeId,
                request,
                RewardBatchTrxStatus.SUSPENDED,
                generateReasonDto(request),
                checksErrorMapper.toModel(request.getChecksError())
        );
    }

    private static ReasonDTO generateReasonDto(TransactionsRequest request) {
        LocalDateTime now = LocalDateTime.now(ZONEID);
        return new ReasonDTO(now, request.getReason());
    }

    void validChecksError(ChecksErrorDTO dto) {
        if (dto == null) return;

        boolean anyTrue =
                dto.isCfError() ||
                        dto.isProductEligibilityError() ||
                        dto.isDisposalRaeeError() ||
                        dto.isPriceError() ||
                        dto.isBonusError() ||
                        dto.isSellerReferenceError() ||
                        dto.isAccountingDocumentError() ||
                        dto.isGenericError();

        if (!anyTrue) {
            throw new InvalidChecksErrorException(ERROR_MESSAGE_INVALID_CHECKS_ERROR);
        }
    }


    @Override
    public Mono<RewardBatch> rejectTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request) {
        validChecksError(request.getChecksError());
        return updateTransactionStatuses(
                rewardBatchId,
                initiativeId,
                request,
                RewardBatchTrxStatus.REJECTED,
                generateReasonDto(request),
                checksErrorMapper.toModel(request.getChecksError())
        );
    }

    @Override
    public Mono<RewardBatch> approvedTransactions(String rewardBatchId, TransactionsRequest request, String initiativeId) {
        return updateTransactionStatuses(
                rewardBatchId,
                initiativeId,
                request,
                RewardBatchTrxStatus.APPROVED,
                null,
                null
        );
    }

    @Override
    public Mono<Long> evaluatingRewardBatches(List<String> rewardBatchesRequest, String initiativeId) {
        log.info("[EVALUATING_REWARD_BATCH] Starting evaluation of reward batches with status SENT");
        Flux<RewardBatch> rewardBatchToElaborate;
        if (rewardBatchesRequest == null) {
            rewardBatchToElaborate = rewardBatchLifecyclePort.findBatchesWithStatus(RewardBatchStatus.SENT, initiativeId);
        } else {
            rewardBatchToElaborate = Flux.fromIterable(rewardBatchesRequest)
                    .flatMap(batchId -> rewardBatchLifecyclePort.findBatchWithStatus(batchId, initiativeId, RewardBatchStatus.SENT));
        }

        return rewardBatchToElaborate
                .flatMap(rewardBatch -> {
                    log.info(
                            "[EVALUATING_REWARD_BATCH] Evaluating reward batch {}",
                            Utilities.sanitizeString(rewardBatch.getId())
                    );
                    return rewardBatchTransactionDecisionPort.prepareEvaluation(
                            rewardBatch.getId(),
                            initiativeId
                    );
                })
                .count()
                .doOnSuccess(count ->
                        log.info("[EVALUATING_REWARD_BATCH] Completed evaluation. Total batches processed: {}", count));
    }

    private Mono<RewardBatch> updateTransactionStatuses(
            String rewardBatchId,
            String initiativeId,
            TransactionsRequest request,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            ChecksError checksError
    ) {
        return rewardBatchLifecyclePort
                .findBatchWithStatus(rewardBatchId, initiativeId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage
                                .ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH
                                .formatted(rewardBatchId)
                )))
                .flatMap(batch -> Flux.fromIterable(request.getTransactionIds())
                        .flatMap(transactionId -> rewardBatchTransactionDecisionPort
                                .updateStatusAndReturnOld(
                                        initiativeId,
                                        rewardBatchId,
                                        transactionId,
                                        newStatus,
                                        reason,
                                        batch.getMonth(),
                                        checksError
                                ))
                        .then(Mono.fromRunnable(() -> auditUtilities.logTransactionsStatusChanged(
                                newStatus.name(),
                                initiativeId,
                                request.getTransactionIds().toString(),
                                request.getChecksError()
                        )))
                        .then(rewardBatchLifecyclePort.findBatch(rewardBatchId, initiativeId)));
    }

    @Override
    public Mono<DownloadRewardBatchResponseDTO> downloadApprovedRewardBatchFile(String merchantId, String organizationRole, String initiativeId, String rewardBatchId) {

        if ((merchantId == null || merchantId.isBlank()) &&
                (organizationRole == null || organizationRole.isBlank())) {
            return Mono.error(new RewardBatchInvalidRequestException(MERCHANT_OR_OPERATOR_HEADER_MANDATORY));
        }

        Mono<RewardBatch> query =
                merchantId == null
                        ? rewardBatchLifecyclePort.findBatch(rewardBatchId)
                        : merchantRewardBatchLookupPort.findMerchantBatch(merchantId, initiativeId, rewardBatchId);

        return query
                .switchIfEmpty(Mono.error(new RewardBatchNotFound(
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(batch -> {

                    if (merchantId == null && !isValidInvitaliaOperator(organizationRole)) {
                        throw new RoleNotAllowedException(
                                ROLE_NOT_ALLOWED,
                                ERROR_MESSAGE_ROLE_NOT_ALLOWED
                        );
                    }

                    if (!RewardBatchStatus.APPROVED.equals(batch.getStatus()) && !isRefundState(batch.getStatus())) {
                        throw new RewardBatchNotApprovedException(
                                REWARD_BATCH_NOT_APPROVED_OR_REFUNDABLE,
                                ERROR_MESSAGE_REWARD_BATCH_NOT_APPROVED_OR_REFUNDABLE.formatted(rewardBatchId)
                        );
                    }

                    String filename = batch.getFilename();
                    if (filename == null || filename.isBlank()) {
                        throw new RewardBatchMissingFilenameException(
                                REWARD_BATCH_MISSING_FILENAME,
                                ERROR_MESSAGE_REWARD_BATCH_MISSING_FILENAME.formatted(rewardBatchId)
                        );
                    }

                    String blobPath = String.format(
                            REWARD_BATCHES_PATH_STORAGE_FORMAT+ "%s",
                            initiativeId,
                            batch.getMerchantId(),
                            rewardBatchId,
                            filename
                    );

                    return approvedRewardBatchBlobService.getFileSignedUrl(blobPath)
                            .map(approvedBatchUrl -> DownloadRewardBatchResponseDTO.builder()
                                    .approvedBatchUrl(approvedBatchUrl)
                                    .build());
                });
    }

    private boolean isValidInvitaliaOperator(String organizationRole) {
        return OPERATOR_1.equals(organizationRole)
                || OPERATOR_2.equals(organizationRole)
                || OPERATOR_3.equals(organizationRole);
    }

    private boolean isRefundState(RewardBatchStatus status) {
        return Set.of(
                RewardBatchStatus.PENDING_REFUND,
                RewardBatchStatus.NOT_REFUNDED,
                RewardBatchStatus.REFUNDED
        ).contains(status);
    }

    @Override
    public Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId) {
        return rewardBatchLifecyclePort.findBatch(rewardBatchId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.EVALUATING)
                        && rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(rewardBatch -> {
                    Flux<RewardBatch> previousBatchesFlux = rewardBatchListPort.findBatchesBeforeMonth(
                            rewardBatch.getMerchantId(),
                            rewardBatch.getInitiativeId(),
                            rewardBatch.getPosType(),
                            rewardBatch.getMonth());
                    Mono<Boolean> hasUnapprovedBatch = previousBatchesFlux
                            .filter(batch -> !batch.getStatus().equals(RewardBatchStatus.APPROVED) && !isRefundState(batch.getStatus()))
                            .hasElements();
                    return hasUnapprovedBatch
                            .flatMap(isUnapprovedPresent ->
                                    Boolean.TRUE.equals(isUnapprovedPresent)
                                            ? Mono.error(new ClientExceptionWithBody(
                                            BAD_REQUEST,
                                            REWARD_BATCH_INVALID_REQUEST,
                                            ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE.formatted(rewardBatchId)
                                    ))
                                            : Mono.just(rewardBatch)
                            );
                })
                .map(rewardBatch -> {
                    LocalDateTime nowDateTime = LocalDateTime.now(ZONEID);
                    rewardBatch.setStatus(RewardBatchStatus.APPROVING);
                    rewardBatch.setApprovalDate(nowDateTime);
                    rewardBatch.setUpdateDate(nowDateTime);
                    return rewardBatch;
                })
                .flatMap(rewardBatchLifecyclePort::saveBatch);
    }


    @Override
    public Mono<Void> rewardBatchConfirmationBatch(String initiativeId, List<String> rewardBatchIds) {
        if (rewardBatchIds != null && !rewardBatchIds.isEmpty()) {
            return processBatchesOrchestrator(
                    initiativeId,
                    rewardBatchIds,
                    this::processSingleBatchConfirmation
            );
        }

        return processBatchesByStatusPaginated(
                initiativeId,
                RewardBatchStatus.APPROVING,
                pageable -> rewardBatchLifecyclePort.findBatchesWithStatus(
                        RewardBatchStatus.APPROVING,
                        initiativeId,
                        pageable
                ),
                this::processSingleBatchConfirmation
        );
    }

    @Override
    public Mono<RewardBatch> updateBatch(RewardBatch batch, InvitaliaOutcomeResponseDTO response) {
        String status = response.getErogazione().getStatus();

        if (InvitaliaOutcomeStatus.COMPLETATA.name().equalsIgnoreCase(status)) {
            return rewardBatchDeliveryPort.recordRefundOutcome(
                            batch.getId(),
                            batch.getInitiativeId(),
                            RewardBatchStatus.REFUNDED,
                            response.getErogazione().getDateValue(),
                            null
                    )
                    .switchIfEmpty(outcomePersistenceStateMismatch(batch.getId(), "refund completion"))
                    .doOnNext(this::logOutcomeTransition);

        }
        if (InvitaliaOutcomeStatus.RIFIUTATA.name().equalsIgnoreCase(status)) {
            String errorMessage = response.getErrors() == null || response.getErrors().isEmpty()
                    ? null
                    : response.getErrors().stream()
                    .map(error -> error.getCode() + " - " + error.getMessage())
                    .reduce((first, second) -> first + "; " + second)
                    .orElse(null);

            return rewardBatchDeliveryPort.recordRefundOutcome(
                            batch.getId(),
                            batch.getInitiativeId(),
                            RewardBatchStatus.NOT_REFUNDED,
                            null,
                            errorMessage
                    )
                    .switchIfEmpty(outcomePersistenceStateMismatch(batch.getId(), "refund rejection"))
                    .doOnNext(this::logOutcomeTransition);
        }
        if (InvitaliaOutcomeStatus.IN_LAVORAZIONE.name().equalsIgnoreCase(status)
                || InvitaliaOutcomeStatus.ERRORE.name().equalsIgnoreCase(status)) {
            log.info("Batch {} has not been processed with status {}, the external status is {}", batch.getId(), batch.getStatus(), status);
            return Mono.just(batch);
        }

        return Mono.just(batch);
    }

    private void logOutcomeTransition(RewardBatch batch) {
        log.info("Batch {} outcome processed, setting status {}", batch.getId(), batch.getStatus());
    }

    private Mono<RewardBatch> outcomePersistenceStateMismatch(String rewardBatchId, String outcomeType) {
        log.warn("Unable to persist {} for batch {} because its lifecycle state changed",
                outcomeType,
                Utilities.sanitizeString(rewardBatchId));
        return Mono.error(new ClientExceptionWithBody(
                BAD_REQUEST,
                REWARD_BATCH_INVALID_REQUEST,
                ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
        ));
    }

    @Override
    public Mono<Void> checkRewardBatchesOutcomes(String initiativeId, List<String> rewardBatchIds) {

        List<String> batchIds = rewardBatchIds != null ? rewardBatchIds : List.of();

        Flux<RewardBatch> batches;

        if (batchIds.isEmpty()) {
            batches = rewardBatchLifecyclePort.findBatchesWithStatus(RewardBatchStatus.PENDING_REFUND, initiativeId);
        } else {
            batches = Flux.fromIterable(batchIds)
                    .flatMap(batchId ->
                            rewardBatchLifecyclePort.findBatchWithStatus(batchId, initiativeId, RewardBatchStatus.PENDING_REFUND)
                    );
        }

        return batches
                .flatMap(batch ->
                        erogazioniRestClient.getOutcome(batch.getId())
                                .flatMap(outcome -> updateBatch(batch, outcome))
                )
                .then();
    }

    @Override
    public Mono<Void> rewardBatchDeliveryBatch(String initiativeId, List<String> rewardBatchIds) {
        if (rewardBatchIds != null && !rewardBatchIds.isEmpty()) {
            return processBatchesOrchestrator(
                    initiativeId,
                    rewardBatchIds,
                    this::processSingleBatchDelivery
            );
        }

        return processBatchesByStatusPaginated(
                initiativeId,
                RewardBatchStatus.APPROVED,
                pageable -> rewardBatchLifecyclePort.findDeliverableBatches(initiativeId, pageable),
                this::processSingleBatchDelivery
        );
    }

    private Mono<Void> processBatchesOrchestrator(
            String initiativeId,
            List<String> rewardBatchIds,
            BiFunction<RewardBatch, String, Mono<?>> businessLogic) {

        return Flux.fromIterable(rewardBatchIds)
                .concatMap(batchId ->
                        rewardBatchLifecyclePort.findBatch(batchId, initiativeId)
                )
                .concatMap(batch -> processBatch(batch, initiativeId, businessLogic))
                .then();
    }

    private Mono<Void> processBatchesByStatusPaginated(
            String initiativeId,
            RewardBatchStatus status,
            Function<Pageable, Flux<RewardBatch>> batchFinder,
            BiFunction<RewardBatch, String, Mono<?>> businessLogic) {

        return batchFinder.apply(Pageable.ofSize(pagesize))
                .collectList()
                .flatMap(batchList -> {
                    if (batchList.isEmpty()) {
                        log.info("No more batches found with status {} to process.", status);
                        return Mono.empty();
                    }

                    log.info("Found {} batches with status {} to process in current page.",
                            batchList.size(), status);

                    return Flux.fromIterable(batchList)
                            .concatMap(batch -> processBatch(batch, initiativeId, businessLogic))
                            .then(Mono.defer(() ->
                                    processBatchesByStatusPaginated(
                                            initiativeId,
                                            status,
                                            batchFinder,
                                            businessLogic
                                    )
                            ));
                });
    }

    private Mono<?> processBatch(
            RewardBatch batch,
            String initiativeId,
            BiFunction<RewardBatch, String, Mono<?>> businessLogic) {

        log.info(PROCESSING_BATCH_LOG, batch.getId());

        return businessLogic.apply(batch, initiativeId)
                .onErrorResume(error -> {
                    log.error(
                            FAILED_TO_PROCESS_BATCH_LOG,
                            batch.getId(),
                            error.getMessage(),
                            error
                    );
                    return Mono.empty();
                });
    }


    public Mono<RewardBatch> processSingleBatchConfirmation(RewardBatch batch, String initiativeId) {
        String rewardBatchId = batch.getId();

        return rewardBatchLifecyclePort.findBatch(rewardBatchId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.APPROVING)
                        && rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(ignored -> prepareFinalApproval(rewardBatchId, initiativeId))
                .flatMap(preparedBatch -> handleSuspendedTransactions(preparedBatch, initiativeId))
                .flatMap(ignored -> rewardBatchFinalApprovalPort.completeFinalApproval(rewardBatchId, initiativeId)
                        .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                                BAD_REQUEST,
                                REWARD_BATCH_INVALID_REQUEST,
                                ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                        ))))
                .flatMap(savedBatch ->
                        this.generateAndSaveCsv(rewardBatchId, initiativeId, savedBatch.getMerchantId())
                                .onErrorResume(e -> {
                                    log.error("Critical error while generating CSV for batch {}", Utilities.sanitizeString(rewardBatchId), e);
                                    return Mono.just("ERROR");
                                })
                                .thenReturn(savedBatch)
                );
    }

    public Mono<RewardBatch> processSingleBatchDelivery(RewardBatch batch, String initiativeId) {
        String rewardBatchId = batch.getId();

        return rewardBatchLifecyclePort.findBatch(rewardBatchId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
                .then(rewardBatchDeliveryPort.snapshotDeliveryAmount(rewardBatchId, initiativeId))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_OR_AMOUNT_BATCH.formatted(rewardBatchId))))
                .flatMap(rewardBatch -> merchantRestClient.getMerchantDetail(rewardBatch.getMerchantId(), initiativeId)
                        .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                                HttpStatus.NOT_FOUND,
                                MERCHANT_NOT_FOUND,
                                ERROR_MESSAGE_MERCHANT_NOT_FOUND.formatted(rewardBatch.getMerchantId(), initiativeId))))
                        .flatMap(merchantDetail -> selfcareInstitutionsRestClient.getInstitutions(merchantDetail.getFiscalCode())
                                .flatMap(institutionList -> {
                                    if (institutionList.getInstitutions() == null || institutionList.getInstitutions().isEmpty()) {
                                        return Mono.error(new ClientExceptionWithBody(
                                                HttpStatus.NOT_FOUND,
                                                MERCHANT_NOT_FOUND_IN_SELFCARE,
                                                ERROR_MESSAGE_MERCHANT_NOT_FOUND_IN_SELFCARE.formatted(merchantDetail.getFiscalCode())));
                                    }
                                    if (institutionList.getInstitutions().size() > 1) {
                                        return Mono.error(new ClientExceptionWithBody(
                                                HttpStatus.CONFLICT,
                                                AMBIGUOUS_MERCHANT_DATA_IN_SELFCARE,
                                                ERROR_MESSAGE_AMBIGUOUS_MERCHANT_DATA_IN_SELFCARE.formatted(merchantDetail.getFiscalCode())));
                                    }

                                    InstitutionDTO institution = institutionList.getInstitutions().getFirst();

                                    DeliveryRequest deliveryRequest = DeliveryRequest.builder()
                                            .id(rewardBatchId)
                                            .anagrafica(AnagraficaDTO.builder()
                                                    .partitaIvaCliente(merchantDetail.getVatNumber())
                                                    .codiceFiscaleCliente(merchantDetail.getFiscalCode())
                                                    .ragioneSocialeIntestatario(merchantDetail.getBusinessName())
                                                    .cap(institution.getZipCode())
                                                    .indirizzo(institution.getAddress())
                                                    .localita(institution.getCity())
                                                    .provincia(institution.getCounty())
                                                    .pec(institution.getDigitalAddress())
                                                    .build())
                                            .erogazione(ErogazioneDTO.builder()
                                                    .idPratica(rewardBatchId)
                                                            .dataAmmissione(rewardBatch.getApprovalDate())
                                                    .ibanBeneficiario(merchantDetail.getIban())
                                                            .importo(rewardBatch.getDeliveryAmountCents() / 100.0)
                                                    .intestatarioContoCorrente(merchantDetail.getIbanHolder())
                                                    .build())
                                            .build();

                                    return erogazioniRestClient.postErogazione(deliveryRequest)
                                                    .flatMap(outcome -> rewardBatchDeliveryPort.recordDeliveryOutcome(
                                                            rewardBatchId,
                                                            initiativeId,
                                                            outcome
                                                            )
                                                            .switchIfEmpty(outcomePersistenceStateMismatch(
                                                                    rewardBatchId,
                                                                    "delivery outcome"
                                                            )))
                                                    .doOnNext(updated -> {
                                                        if (updated.getStatus() == RewardBatchStatus.PENDING_REFUND) {
                                                            log.info("[PROCESS_BATCH] Batch {} delivery succeeded. Status moved to PENDING_REFUND", rewardBatchId);
                                                        } else {
                                                            log.warn("[PROCESS_BATCH] Batch {} delivery rejected by server: {}",
                                                                    rewardBatchId,
                                                                    outcomeMessage(updated));
                                                        }
                                                    });
                                })));
    }

    private static String outcomeMessage(RewardBatch batch) {
        return batch.getDeliveryOutcome() == null ? null : batch.getDeliveryOutcome().getMessage();
    }

    private Mono<RewardBatch> handleSuspendedTransactions(RewardBatch originalBatch, String initiativeId) {
        if (originalBatch.getNumberOfTransactionsSuspended() == null
                || originalBatch.getNumberOfTransactionsSuspended() <= 0) {
            log.info("numberOfTransactionSuspended = 0 for batch {}", originalBatch.getId());
            return Mono.just(originalBatch);
        }

        return suspendedTransactionReassignmentPort.reassignSuspendedTransactions(
                        originalBatch.getId(),
                        initiativeId
                )
                .thenReturn(originalBatch);
    }

    public String addOneMonth(String yearMonthString) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
        YearMonth yearMonth = YearMonth.parse(yearMonthString, inputFormatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(inputFormatter);
    }

    public String getTargetMonth(String yearMonthBatchOriginal) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
        YearMonth originalBatchMonth = YearMonth.parse(yearMonthBatchOriginal, formatter);
        YearMonth currentMonth = YearMonth.now(ZONEID);
        YearMonth targetMonth = originalBatchMonth.isAfter(currentMonth) ? originalBatchMonth : currentMonth;
        return targetMonth.format(formatter);
    }


    public String addOneMonthToItalian(String italianMonthString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM yyyy", Locale.ITALIAN);
        YearMonth yearMonth = YearMonth.parse(italianMonthString, formatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(formatter);
    }

    public  Mono<Void> updateAndSaveRewardTransactionsToApprove(String oldBatchId, String initiativeId) {
        return prepareFinalApproval(oldBatchId, initiativeId)
                .doOnNext(ignored -> log.info("Approved pending transactions for batch {}",
                        Utilities.sanitizeString(oldBatchId)))
                .then();

    }

    private Mono<RewardBatch> prepareFinalApproval(String rewardBatchId, String initiativeId) {
        return rewardBatchFinalApprovalPort.prepareFinalApproval(rewardBatchId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )));
    }

    @Override
    public Mono<RewardBatch> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId) {
        return rewardBatchAssigneePromotionPort.findBatchForPromotion(rewardBatchId, initiativeId)
                .switchIfEmpty(Mono.error(new RewardBatchNotFound(
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(batch -> {

                    RewardBatchAssignee assignee = batch.getAssigneeLevel();

                    if (assignee == RewardBatchAssignee.L1) {

                        if (!OPERATOR_1.equals(organizationRole)) {
                            return Mono.error(new RoleNotAllowedForL1PromotionException(
                                    ROLE_NOT_ALLOWED_FOR_L1_PROMOTION,
                                    ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L1_PROMOTION
                            ));
                        }

                        return rewardBatchAssigneePromotionPort.promote(
                                        rewardBatchId,
                                        initiativeId,
                                        RewardBatchAssignee.L1,
                                        RewardBatchAssignee.L2
                                )
                                .switchIfEmpty(Mono.error(new InvalidBatchStateForPromotionException(
                                        INVALID_BATCH_STATE_FOR_PROMOTION,
                                        ERROR_MESSAGE_INVALID_BATCH_STATE_FOR_PROMOTION
                                )));
                    }

                    if (assignee == RewardBatchAssignee.L2) {

                        if (!OPERATOR_2.equals(organizationRole)) {
                            return Mono.error(new RoleNotAllowedForL2PromotionException(
                                    ROLE_NOT_ALLOWED_FOR_L2_PROMOTION,
                                    ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L2_PROMOTION
                            ));
                        }

                        return rewardBatchAssigneePromotionPort.promote(
                                        rewardBatchId,
                                        initiativeId,
                                        RewardBatchAssignee.L2,
                                        RewardBatchAssignee.L3
                                )
                                .switchIfEmpty(Mono.error(new InvalidBatchStateForPromotionException(
                                        INVALID_BATCH_STATE_FOR_PROMOTION,
                                        ERROR_MESSAGE_INVALID_BATCH_STATE_FOR_PROMOTION
                                )));
                    }

                    return Mono.error((new InvalidBatchStateForPromotionException(
                            INVALID_BATCH_STATE_FOR_PROMOTION,
                            ERROR_MESSAGE_INVALID_BATCH_STATE_FOR_PROMOTION
                    )));
                });
    }


    @Override
    public Mono<String> generateAndSaveCsv(String rewardBatchId, String initiativeId, String merchantId) {

        log.info("[GENERATE_AND_SAVE_CSV] Generate CSV for initiative {} and batch {}",
                Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(rewardBatchId) );

        if (rewardBatchId.contains("..") || rewardBatchId.contains("/") || rewardBatchId.contains("\\"))
        {
            log.error("Invalid rewardBatchId for CSV filename: {}", Utilities.sanitizeString(rewardBatchId));
            return Mono.error(new IllegalArgumentException("Invalid batch id for CSV file generation"));
        }

        return rewardBatchLifecyclePort.findBatch(rewardBatchId)
                .switchIfEmpty(Mono.defer(() -> {
                    log.error("[GENERATE_AND_SAVE_CSV] Batch {} not found during CSV generation", Utilities.sanitizeString(rewardBatchId));
                    return Mono.error(new ClientExceptionWithBody(
                            NOT_FOUND,
                            REWARD_BATCH_NOT_FOUND,
                            ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(Utilities.sanitizeString(rewardBatchId))));
                }))
                .flatMap(batch -> {

                    String pathPrefix = String.format(REWARD_BATCHES_PATH_STORAGE_FORMAT,
                            Utilities.sanitizeString(initiativeId),
                            Utilities.sanitizeString(batch.getMerchantId()),
                            Utilities.sanitizeString(rewardBatchId));

                    String reportFilename = String.format(REWARD_BATCHES_REPORT_NAME_FORMAT,
                            batch.getBusinessName(),
                            batch.getName(),
                            batch.getPosType().getDescription()).trim();

                    String filename = pathPrefix + reportFilename;

                    Flux<RewardTransaction> transactionFlux = rewardBatchTransactionReadPort.findBatchTransactions(
                            rewardBatchId, initiativeId, List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED));

                    Flux<String> csvRowsFlux = transactionFlux
                            .concatMap(transaction -> {
                                if (transaction.getFiscalCode() == null || transaction.getFiscalCode().isEmpty()) {
                                    return userRestClient.retrieveUserInfo(transaction.getUserId())
                                            .map(cf -> {
                                                transaction.setFiscalCode(cf.getPii());
                                                return this.mapTransactionToCsvRow(transaction, initiativeId);});
                                } else {
                                    return Mono.just(this.mapTransactionToCsvRow(transaction, initiativeId));
                                }
                            });

                    Flux<String> fullCsvFlux = Flux.just(CSV_HEADER).concatWith(csvRowsFlux);

                    return fullCsvFlux
                            .collect(StringBuilder::new, (sb, s) -> sb.append(s).append("\n"))
                            .map(StringBuilder::toString)
                            .flatMap(csvContent -> this.uploadCsvToBlob(filename, csvContent))
                            .flatMap(uploadedPath -> {
                                batch.setFilename(reportFilename);
                                log.info("Updated batch {} with filename: {}", Utilities.sanitizeString(rewardBatchId), reportFilename);
                                return rewardBatchLifecyclePort.saveBatch(batch)
                                        .thenReturn(reportFilename);
                            });
                })
                .doOnSuccess(result -> log.info("[GENERATE_AND_SAVE_CSV] CSV generation completed successfully for batch: {}", Utilities.sanitizeString(rewardBatchId)));
    }

    private String mapTransactionToCsvRow(RewardTransaction trx, String initiativeId) {

        Function<LocalDateTime, String> safeDateToString =
                date -> date != null
                        ? date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm"))
                        : "";

        LongFunction<String> centsToEuroString = cents -> {
            NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ITALY);
            numberFormat.setMinimumFractionDigits(2);
            numberFormat.setMaximumFractionDigits(2);
            return numberFormat.format(cents / 100.0);
        };

        String productName = trx.getAdditionalProperties() != null &&
                trx.getAdditionalProperties().get("productName") != null
                ? trx.getAdditionalProperties().get("productName")
                : "";
        String productGtin = trx.getAdditionalProperties() != null &&
                trx.getAdditionalProperties().get("productGtin") != null
                ? trx.getAdditionalProperties().get("productGtin")
                : "";

        String productInfo = productName + "\n" + productGtin;

        String invoiceNumber =
                trx.getInvoiceData() != null && trx.getInvoiceData().getDocNumber() != null
                        ? trx.getInvoiceData().getDocNumber()
                        : "";

        return String.join(";",
                safeDateToString.apply(trx.getTrxChargeDate()),
                csvField(productInfo),
                csvField(trx.getFiscalCode()),
                csvField(trx.getId()),
                csvField(trx.getTrxCode()),
                trx.getEffectiveAmountCents() != null
                        ? csvField(centsToEuroString.apply(trx.getEffectiveAmountCents()))
                        : "",
                trx.getRewards().get(initiativeId).getAccruedRewardCents() != null
                        ? csvField(centsToEuroString.apply(
                        trx.getRewards().get(initiativeId).getAccruedRewardCents()))
                        : "",
                csvField(invoiceNumber),
                csvField(trx.getInvoiceData().getFilename()),
                csvField(trx.getRewardBatchTrxStatus().getDescription()),
                csvField(trx.getFranchiseName())
        );
    }

    private String csvField(String s) {
        if (s == null) {
            return "";
        }

        String escaped = s.replace("\"", "\"\"");

        boolean mustQuote =
                escaped.contains(";") ||
                        escaped.contains(",") ||
                        escaped.contains("\n") ||
                        escaped.contains("\r") ||
                        escaped.contains("\"");

        return mustQuote ? "\"" + escaped + "\"" : escaped;
    }

    public Mono<String> uploadCsvToBlob(String filename, String csvContent) {

        return Mono.defer(() -> approvedRewardBatchBlobService.upload(
                        new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8)),
                        filename,
                        "text/csv; charset=UTF-8"
                )
                .flatMap(response -> {
                    if (response.getStatusCode() != HttpStatus.CREATED.value()) {
                        log.error("Error uploading file to storage for file [{}]",
                                Utilities.sanitizeString(filename));
                        return Mono.error(new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
                                ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                                "Error uploading csv file"));
                    }
                    return Mono.just(filename);
                }))
                .onErrorMap(BlobStorageException.class, e -> {
                    log.error("Azure Blob Storage upload failed for file {}", filename, e);
                    return new RuntimeException("Error uploading CSV to Blob Storage.", e);
                });
    }


    @Override
    public Mono<Void> postponeTransaction(String merchantId, String initiativeId, String rewardBatchId, String transactionId) {
        return getInitiativeDataForPostpone(initiativeId)
                .flatMap(initiative -> merchantTransactionPostponementPort.postponeTransaction(
                        merchantId,
                        initiativeId,
                        rewardBatchId,
                        transactionId,
                        initiative.getFruitionEndDate()
                ))
                .then();
    }

    private Mono<InitiativeDetailDTO> getInitiativeDataForPostpone(String initiativeId) {
        return initiativeDataService.getInitiativeData(initiativeId)
                .onErrorResume(error -> mapInitiativeDataError(initiativeId, error));
    }

    private Mono<InitiativeDetailDTO> mapInitiativeDataError(String initiativeId, Throwable error) {
        log.error("[POSTPONE_TRANSACTION] Failed to retrieve initiative data for initiativeId={}", initiativeId, error);

        if (error instanceof InitiativeNotFoundException) {
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.NOT_FOUND,
                    ExceptionCode.GENERIC_ERROR,
                    "Initiative not found: " + initiativeId
            ));
        }

        return Mono.error(new ClientExceptionWithBody(
                HttpStatus.INTERNAL_SERVER_ERROR,
                ExceptionCode.GENERIC_ERROR,
                "Failed to retrieve initiative data: " + error.getMessage()
        ));
    }

    @Data
    public static class TotalAmount {
        private long total;
    }

}
