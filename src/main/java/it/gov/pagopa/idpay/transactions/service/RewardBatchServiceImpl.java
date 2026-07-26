package it.gov.pagopa.idpay.transactions.service;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.mongodb.client.result.DeleteResult;
import com.nimbusds.jose.util.Pair;
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
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.batch.TrxSuspendedBatchInfo;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.*;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantRewardBatchLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchLifecyclePort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchListPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionDecisionPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {

    private static final String DATE_FORMAT = "yyyy-MM";
    private final RewardBatchRepository rewardBatchRepository;
    private final RewardBatchLifecyclePort rewardBatchLifecyclePort;
    private final RewardBatchListPort rewardBatchListPort;
    private final MerchantRewardBatchLookupPort merchantRewardBatchLookupPort;
    private final RewardTransactionRepository rewardTransactionRepository;
    private final RewardBatchTransactionReadPort rewardBatchTransactionReadPort;
    private final RewardBatchTransactionDecisionPort rewardBatchTransactionDecisionPort;
    private final UserRestClient userRestClient;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
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
    private static final DateTimeFormatter BATCH_MONTH_FORMAT = DateTimeFormatter.ofPattern(DATE_FORMAT, Locale.ITALIAN);

    private static final String PROCESSING_BATCH_LOG = "Processing batch {}";
    private static final String FAILED_TO_PROCESS_BATCH_LOG = "Failed to process batch {}: {}";

    public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository,
                                  RewardBatchLifecyclePort rewardBatchLifecyclePort,
                                  RewardBatchListPort rewardBatchListPort,
                                  MerchantRewardBatchLookupPort merchantRewardBatchLookupPort,
                                  RewardTransactionRepository rewardTransactionRepository,
                                  RewardBatchTransactionReadPort rewardBatchTransactionReadPort,
                                  RewardBatchTransactionDecisionPort rewardBatchTransactionDecisionPort,
                                  UserRestClient userRestClient,
                                  ApprovedRewardBatchBlobService approvedRewardBatchBlobService,
                                  ReactiveMongoTemplate reactiveMongoTemplate,
                                  ChecksErrorMapper checksErrorMapper,
                                  AuditUtilities auditUtilities,
                                  MerchantRestClient merchantRestClient,
                                  SelfcareInstitutionsRestClient selfcareInstitutionsRestClient,
                                  ErogazioniRestClient erogazioniRestClient,
                                  InitiativeDataService initiativeDataService,
                                  @Value("${app.batch.paginationSize}") int pagesize) {
        this.rewardBatchRepository = rewardBatchRepository;
        this.rewardBatchLifecyclePort = rewardBatchLifecyclePort;
        this.rewardBatchListPort = rewardBatchListPort;
        this.merchantRewardBatchLookupPort = merchantRewardBatchLookupPort;
        this.rewardTransactionRepository = rewardTransactionRepository;
        this.rewardBatchTransactionReadPort = rewardBatchTransactionReadPort;
        this.rewardBatchTransactionDecisionPort = rewardBatchTransactionDecisionPort;
        this.userRestClient = userRestClient;
        this.approvedRewardBatchBlobService = approvedRewardBatchBlobService;
        this.reactiveMongoTemplate = reactiveMongoTemplate;
        this.checksErrorMapper = checksErrorMapper;
        this.auditUtilities = auditUtilities;
        this.merchantRestClient = merchantRestClient;
        this.selfcareInstitutionsRestClient = selfcareInstitutionsRestClient;
        this.erogazioniRestClient = erogazioniRestClient;
        this.initiativeDataService = initiativeDataService;
        this.pagesize = pagesize;
    }

    @Override
    public Mono<RewardBatch> findOrCreateBatch(String initiativeId, String merchantId, PosType posType, String month, String businessName) {
        return rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(initiativeId, merchantId, posType,
                        month)
                .switchIfEmpty(Mono.defer(() ->
                        createBatch(merchantId, posType, month, businessName, initiativeId)
                                .doOnSuccess(batch -> log.info("[REWARD_BATCH_REPOSITORY]- findOrCreateBatch - created new batch with id: {}, month: {}",
                                        batch.getId(), batch.getMonth()))
                                .onErrorResume(DuplicateKeyException.class, ex ->
                                        rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(initiativeId,
                                                merchantId, posType, month))));
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

    private Mono<RewardBatch> createBatch(String merchantId, PosType posType, String month, String businessName, String initiativeId) {

        YearMonth batchYearMonth = YearMonth.parse(month);
        LocalDateTime startDate = batchYearMonth.atDay(1).atTime(0,0,0);
        LocalDateTime endDate = batchYearMonth.atEndOfMonth().atTime(23,59,59);

        RewardBatch batch = RewardBatch.builder()
                .merchantId(merchantId)
                .businessName(businessName)
                .month(month)
                .posType(posType)
                .status(RewardBatchStatus.CREATED)
                .partial(false)
                .name(buildBatchName(batchYearMonth))
                .startDate(startDate)
                .endDate(endDate)
                .approvedAmountCents(0L)
                .suspendedAmountCents(0L)
                .initialAmountCents(0L)
                .numberOfTransactions(0L)
                .numberOfTransactionsElaborated(0L)
                .reportPath(null)
                .assigneeLevel(RewardBatchAssignee.L1)
                .numberOfTransactionsSuspended(0L)
                .numberOfTransactionsRejected(0L)
                .creationDate(LocalDateTime.now())
                .updateDate(LocalDateTime.now())
                .initiativeId(initiativeId)
                .build();

        return rewardBatchRepository.save(batch);
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
                    if (!YearMonth.now().isAfter(batchMonth)) {
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

                                LocalDateTime dateTimeNow = LocalDateTime.now();
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

        ChecksError checksErrorModel = checksErrorMapper.toModel(request.getChecksError());
        ReasonDTO reason = generateReasonDto(request);

        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(rewardBatchId, initiativeId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds()).map(trxId -> Pair.of(trxId, batch.getMonth())))
                .flatMap(trxId2ActualBatchMonth -> rewardBatchTransactionDecisionPort
                        .updateStatusAndReturnOld(initiativeId, rewardBatchId, trxId2ActualBatchMonth.getLeft(), RewardBatchTrxStatus.SUSPENDED, reason, trxId2ActualBatchMonth.getRight(), checksErrorModel)
                        .map(trxOld -> Pair.of(trxOld, trxId2ActualBatchMonth.getRight()))
                )
                .reduce(BatchCountersDTO.newBatch(), (acc, trxOld2ActualRewardBatch) -> {

                    RewardTransaction trxOld = trxOld2ActualRewardBatch.getLeft();

                    if (trxOld == null) {
                        return acc;
                    }

                    Long accrued = trxOld.getRewards().get(initiativeId) != null
                            ? trxOld.getRewards().get(initiativeId).getAccruedRewardCents()
                            : null;

                    switch (trxOld.getRewardBatchTrxStatus()) {

                        case RewardBatchTrxStatus.SUSPENDED ->
                                suspendedTransactionAlreadySuspended(acc, trxOld2ActualRewardBatch, trxOld);


                        case RewardBatchTrxStatus.APPROVED -> {
                            acc.incrementTrxSuspended();

                            if (accrued != null) {
                                acc.decrementApprovedAmountCents(accrued);
                                acc.incrementSuspendedAmountCents(accrued);
                            }
                        }

                        case RewardBatchTrxStatus.TO_CHECK,
                             RewardBatchTrxStatus.CONSULTABLE -> {
                            acc.incrementTrxElaborated();
                            acc.incrementTrxSuspended();

                            if (accrued != null) {
                                acc.decrementApprovedAmountCents(accrued);
                                acc.incrementSuspendedAmountCents(accrued);
                            }
                        }

                        case RewardBatchTrxStatus.REJECTED -> {
                            acc.decrementTrxRejected();
                            acc.incrementTrxSuspended();
                            if (accrued != null) {
                                acc.incrementSuspendedAmountCents(accrued);
                            }
                        }
                    }

                    return acc;
                })
                .flatMap(acc -> {

                    auditUtilities.logTransactionsStatusChanged(
                            RewardBatchTrxStatus.SUSPENDED.name(),
                            initiativeId,
                            request.getTransactionIds().toString(),
                            request.getChecksError()
                    );

                    return rewardBatchRepository.updateTotals(
                            initiativeId,
                            rewardBatchId,
                            acc);
                });
    }

    private static ReasonDTO generateReasonDto(TransactionsRequest request) {
        LocalDateTime now = LocalDateTime.now();
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


    private void suspendedTransactionAlreadySuspended(BatchCountersDTO acc, Pair<RewardTransaction, String> trxOld2ActualRewardBatch, RewardTransaction trxOld) {
        if(trxOld.getRewardBatchLastMonthElaborated() !=  null &&
                (getYearMonth(trxOld.getRewardBatchLastMonthElaborated()).isBefore(getYearMonth(trxOld2ActualRewardBatch.getRight())))) {
            log.info("Handler counters for transaction {} with status SUSPENDED", trxOld.getId());
            acc.incrementTrxElaborated();
        } else {
            log.info("Skipping  handler  for transaction  {}:  status  is already  SUSPENDED", trxOld.getId());
        }
    }

    @Override
    public Mono<RewardBatch> rejectTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request) {
        validChecksError(request.getChecksError());

        ChecksError checksErrorModel = checksErrorMapper.toModel(request.getChecksError());
        ReasonDTO reason = generateReasonDto(request);

        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                        rewardBatchId,
                        initiativeId,
                        RewardBatchStatus.EVALUATING
                )
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds())
                        .flatMap(trxId -> rewardBatchTransactionDecisionPort
                                .updateStatusAndReturnOld(
                                        initiativeId,
                                        rewardBatchId,
                                        trxId,
                                        RewardBatchTrxStatus.REJECTED,
                                        reason,
                                        batch.getMonth(),
                                        checksErrorModel
                                )
                                .doOnNext(trxOld -> {
                                    if (trxOld != null) {
                                        log.info(
                                                "[REJECT_TRANSACTION] Transaction {} rejected. batchId: {}, initiativeId: {}",
                                                trxOld.getId(),
                                                Utilities.sanitizeString(rewardBatchId),
                                                Utilities.sanitizeString(initiativeId)
                                        );
                                    }
                                })
                        )
                )
                .reduce(BatchCountersDTO.newBatch(),
                        (acc, trxOld) -> buildRejectCounters(acc, trxOld, initiativeId))
                .flatMap(acc -> {
                    auditUtilities.logTransactionsStatusChanged(
                            RewardBatchTrxStatus.REJECTED.name(),
                            initiativeId,
                            request.getTransactionIds().toString(),
                            request.getChecksError()
                    );

                    return rewardBatchRepository.updateTotals(
                            initiativeId,
                            rewardBatchId,
                            acc
                    );
                });
    }

    private BatchCountersDTO buildRejectCounters(BatchCountersDTO acc, RewardTransaction trxOld, String initiativeId) {
        if (trxOld == null) {
            return acc;
        }

        Long accrued = trxOld.getRewards().get(initiativeId) != null
                ? trxOld.getRewards().get(initiativeId).getAccruedRewardCents()
                : null;

        applyCountersByStatus(acc, trxOld, accrued);
        return acc;
    }

    private void applyCountersByStatus(BatchCountersDTO acc, RewardTransaction trxOld, Long accrued) {
        switch (trxOld.getRewardBatchTrxStatus()) {
            case RewardBatchTrxStatus.REJECTED ->
                    log.info("Skipping handler for transaction {}: status is already REJECTED", trxOld.getId());

            case RewardBatchTrxStatus.APPROVED -> {
                acc.incrementTrxRejected();
                if (accrued != null) {
                    acc.decrementApprovedAmountCents(accrued);
                }
            }

            case RewardBatchTrxStatus.TO_CHECK,
                 RewardBatchTrxStatus.CONSULTABLE -> {
                acc.incrementTrxElaborated();
                acc.incrementTrxRejected();

                if (accrued != null) {
                    acc.decrementApprovedAmountCents(accrued);
                }
            }

            case RewardBatchTrxStatus.SUSPENDED -> {
                acc.decrementTrxSuspended();
                acc.incrementTrxRejected();

                if (accrued != null) {
                    acc.decrementSuspendedAmountCents(accrued);
                }
            }
        }
    }

    @Override
    public Mono<RewardBatch> approvedTransactions(String rewardBatchId, TransactionsRequest request, String initiativeId) {
        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(rewardBatchId, initiativeId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds())
                        .map(trxId -> Pair.of(trxId, batch.getMonth())))
                .flatMap(trxIdAndMonthElaborated -> rewardBatchTransactionDecisionPort.updateStatusAndReturnOld(initiativeId, rewardBatchId, trxIdAndMonthElaborated.getLeft(), RewardBatchTrxStatus.APPROVED, null, trxIdAndMonthElaborated.getRight(), null)
                        .map(trxOld -> Pair.of(trxOld, trxIdAndMonthElaborated.getRight())))
                .reduce(BatchCountersDTO.newBatch(), (acc, trxOld2ActualBatchMonth) -> {
                    RewardTransaction trxOld = trxOld2ActualBatchMonth.getLeft();
                    switch (trxOld.getRewardBatchTrxStatus()){

                        case RewardBatchTrxStatus.APPROVED ->
                                log.info("Skipping  handler  for transaction  {}:  status  is already  APPROVED",  trxOld.getId());

                        case RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE ->
                                acc.incrementTrxElaborated();

                        case RewardBatchTrxStatus.SUSPENDED -> {
                            acc.decrementTrxSuspended();
                            if(trxOld.getRewards().get(initiativeId) != null && trxOld.getRewards().get(initiativeId).getAccruedRewardCents() != null) {
                                acc.incrementApprovedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                                acc.decrementSuspendedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                            }
                        }

                        case RewardBatchTrxStatus.REJECTED -> {
                            acc.decrementTrxRejected();
                            if(trxOld.getRewards().get(initiativeId) != null && trxOld.getRewards().get(initiativeId).getAccruedRewardCents() != null) {
                                acc.incrementApprovedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                            }
                        }
                    }
                    return acc;
                })

                .flatMap(acc ->
                        rewardBatchRepository.updateTotals(
                                initiativeId,
                                rewardBatchId,
                                acc)
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
                    log.info("[EVALUATING_REWARD_BATCH] Evaluating reward batch {}", Utilities.sanitizeString(rewardBatch.getId()));
                    return rewardTransactionRepository.rewardTransactionsByBatchIdAndInitiativeId(rewardBatch.getId(), initiativeId)
                            .thenReturn(rewardBatch)
                            .log("[EVALUATING_REWARD_BATCH]Completed evaluation of transactions for reward batch %s".formatted(Utilities.sanitizeString(rewardBatch.getId())));
                })
                .flatMap(batch -> rewardTransactionRepository.sumSuspendedAccruedRewardCents(initiativeId, batch.getId())
                        .map(suspendedAmountCents -> new TrxSuspendedBatchInfo(batch.getId(), batch.getSuspendedAmountCents(), batch.getInitialAmountCents())))
                .flatMap(suspendedInfo -> rewardBatchLifecyclePort.updateEvaluationStatus(suspendedInfo.getRewardBatchId(), initiativeId, suspendedInfo.getInitialRewardBatchAmountCents() - suspendedInfo.getSuspendedRewardAmountCents())
                        .log("[EVALUATING_REWARD_BATCH] Reward batch %s moved to status EVALUATING".formatted(Utilities.sanitizeString(suspendedInfo.getRewardBatchId()))))
                .count()
                .doOnSuccess(count ->
                        log.info("[EVALUATING_REWARD_BATCH] Completed evaluation. Total batches processed: {}", count));
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
                .map(batch -> {

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

                    return DownloadRewardBatchResponseDTO.builder()
                            .approvedBatchUrl(
                                    approvedRewardBatchBlobService.getFileSignedUrl(blobPath)
                            )
                            .build();
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

    private String buildBatchName(YearMonth month) {
        String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
        String year = String.valueOf(month.getYear());

        return String.format("%s %s", monthName, year);
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
                    LocalDateTime nowDateTime = LocalDateTime.now();
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

        batch.setRefundOutcomeTimestamp(LocalDateTime.now());

        String status = response.getErogazione().getStatus();

        if (InvitaliaOutcomeStatus.COMPLETATA.name().equalsIgnoreCase(status)) {
            batch.setStatus(RewardBatchStatus.REFUNDED);
            batch.setRefundValutaDate(response.getErogazione().getDateValue());

        } else if (InvitaliaOutcomeStatus.RIFIUTATA.name().equalsIgnoreCase(status)) {

            batch.setStatus(RewardBatchStatus.NOT_REFUNDED);

            if (response.getErrors() != null && !response.getErrors().isEmpty()) {
                String errorMessage = response.getErrors().stream()
                        .map(error -> error.getCode() + " - " + error.getMessage())
                        .reduce((a, b) -> a + "; " + b)
                        .orElse(null);

                batch.setRefundErrorMessage(errorMessage);

            }

        } else if (InvitaliaOutcomeStatus.IN_LAVORAZIONE.name().equalsIgnoreCase(status) || InvitaliaOutcomeStatus.ERRORE.name().equalsIgnoreCase(status)) {
            log.info("Batch {} has not been processed with status {}, the external status is {}", batch.getId(), batch.getStatus(), status);
            return Mono.just(batch);
        }

        logOutcomeTransition(batch);

        return rewardBatchLifecyclePort.saveBatch(batch);
    }

    private void logOutcomeTransition(RewardBatch batch) {
        log.info("Batch {} outcome processed, setting status {}", batch.getId(), batch.getStatus());
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
                .flatMap(originalBatch -> updateAndSaveRewardTransactionsToApprove(rewardBatchId, initiativeId)
                        .thenReturn(originalBatch))
                .flatMap(originalBatch -> handleSuspendedTransactions(originalBatch, initiativeId))
                .flatMap(originalBatch -> {
                    originalBatch.setStatus(RewardBatchStatus.APPROVED);
                    originalBatch.setUpdateDate(LocalDateTime.now());
                    return rewardBatchLifecyclePort.saveBatch(originalBatch);
                })
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
                .filter(rewardBatch -> RewardBatchStatus.APPROVED.equals(rewardBatch.getStatus()) && rewardBatch.getApprovedAmountCents()>0)
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
                                                    .dataAmmissione(batch.getApprovalDate())
                                                    .ibanBeneficiario(merchantDetail.getIban())
                                                    .importo(batch.getApprovedAmountCents() / 100.0)
                                                    .intestatarioContoCorrente(merchantDetail.getIbanHolder())
                                                    .build())
                                            .build();

                                    return erogazioniRestClient.postErogazione(deliveryRequest)
                                            .flatMap(outcome -> {
                                                batch.setDeliveryOutcome(outcome);
                                                if (outcome.isSucceded()) {
                                                    batch.setStatus(RewardBatchStatus.PENDING_REFUND);
                                                    batch.setDeliveryDateRequest(LocalDateTime.now());
                                                    log.info("[PROCESS_BATCH] Batch {} delivery succeeded. Status moved to PENDING_REFUND", rewardBatchId);
                                                } else {
                                                    log.warn("[PROCESS_BATCH] Batch {} delivery rejected by server: {}", rewardBatchId, outcome.getMessage());
                                                }

                                                return rewardBatchLifecyclePort.saveBatch(batch);
                                            });
                                })));
    }



    private Mono<RewardBatch> handleSuspendedTransactions(RewardBatch originalBatch, String initiativeId) {
        if (originalBatch.getNumberOfTransactionsSuspended() == null
                || originalBatch.getNumberOfTransactionsSuspended() <= 0) {
            log.info("numberOfTransactionSuspended = 0 for batch {}", originalBatch.getId());
            return Mono.just(originalBatch);
        }

        return rewardBatchTransactionReadPort
                .findBatchTransactions(originalBatch.getId(), initiativeId, List.of(RewardBatchTrxStatus.SUSPENDED))
                .count()
                .flatMap(countToMove ->
                        findOrCreateBatch(
                                originalBatch.getInitiativeId(),
                                originalBatch.getMerchantId(),
                                originalBatch.getPosType(),
                                getTargetMonth(originalBatch.getMonth()),
                                originalBatch.getBusinessName()
                        ).flatMap(newBatch ->
                                updateAndSaveRewardTransactionsSuspended(
                                        originalBatch.getId(),
                                        initiativeId,
                                        newBatch.getId(),
                                        originalBatch.getMonth()
                                ).flatMap(totalAccrued -> {
                                    BatchCountersDTO batchCounters = BatchCountersDTO.newBatch()
                                            .incrementInitialAmountCents(totalAccrued)
                                            .incrementTrxElaborated(countToMove)
                                            .incrementNumberOfTransactions(countToMove)
                                            .incrementSuspendedAmountCents(totalAccrued)
                                            .incrementTrxSuspended(countToMove);

                                    return rewardBatchRepository.updateTotals(newBatch.getInitiativeId(), newBatch.getId(), batchCounters)
                                            .then(rewardBatchRepository.updateTotals(
                                                    originalBatch.getInitiativeId(),
                                                    originalBatch.getId(),
                                                    BatchCountersDTO.newBatch().decrementNumberOfTransactions(countToMove)
                                            ));
                                })
                        )
                );
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
        YearMonth currentMonth = YearMonth.now();
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
        List<RewardBatchTrxStatus>  statusList = new ArrayList<>();
        statusList.add(RewardBatchTrxStatus.TO_CHECK);
        statusList.add(RewardBatchTrxStatus.CONSULTABLE);


        return rewardBatchTransactionReadPort.findBatchTransactions(oldBatchId, initiativeId, statusList)
                .collectList()
                .doOnNext(list ->
                        log.info("Found {} transactions to approve for batch {}",
                                list.size(),
                                Utilities.sanitizeString(oldBatchId))
                )
                .flatMapMany(Flux::fromIterable)
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then();

    }

    public Mono<Long> updateAndSaveRewardTransactionsSuspended(String oldBatchId, String initiativeId, String newBatchId, String oldMonth) {
        List<RewardBatchTrxStatus> statusList = List.of(RewardBatchTrxStatus.SUSPENDED);

        return rewardBatchTransactionReadPort.findBatchTransactions(oldBatchId, initiativeId, statusList)
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("No suspended transactions found for the batch {}", Utilities.sanitizeString(oldBatchId));
                    return Flux.empty();
                }))
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchId(newBatchId);
                    rewardTransaction.setStatus(SyncTrxStatus.INVOICED.name());
                    if(rewardTransaction.getRewardBatchLastMonthElaborated() == null) {
                        rewardTransaction.setRewardBatchLastMonthElaborated(oldMonth);
                    }

                    Long rewardCents = 0L;
                    if (rewardTransaction.getRewards() != null &&
                            rewardTransaction.getRewards().get(initiativeId) != null) {
                        rewardCents = rewardTransaction.getRewards().get(initiativeId).getAccruedRewardCents();
                    }

                    return rewardTransactionRepository.save(rewardTransaction)
                            .thenReturn(rewardCents != null ? rewardCents : 0L);
                })
                .reduce(0L, Long::sum)
                .doOnNext(total -> log.info("Total suspended reward cents from old batch {}: {}", newBatchId, total));
    }

    @Override
    public Mono<RewardBatch> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId) {
        return rewardBatchLifecyclePort.findBatch(rewardBatchId)
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

                        long total = batch.getNumberOfTransactions();
                        long elaborated = batch.getNumberOfTransactionsElaborated();

                        if (total > 0 && elaborated < Math.ceil(total * 0.15)) {
                            return Mono.error(new BatchNotElaborated15PercentException(
                                    BATCH_NOT_ELABORATED_15_PERCENT,
                                    ERROR_MESSAGE_BATCH_NOT_ELABORATED_15_PERCENT
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L2);
                        return rewardBatchLifecyclePort.saveBatch(batch);
                    }

                    if (assignee == RewardBatchAssignee.L2) {

                        if (!OPERATOR_2.equals(organizationRole)) {
                            return Mono.error(new RoleNotAllowedForL2PromotionException(
                                    ROLE_NOT_ALLOWED_FOR_L2_PROMOTION,
                                    ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L2_PROMOTION
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L3);
                        return rewardBatchRepository.save(batch);
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

        return Mono.fromCallable(() -> {
                    InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
                    Response<BlockBlobItem> response = approvedRewardBatchBlobService.upload(
                            inputStream,
                            filename,
                            "text/csv; charset=UTF-8"
                    );

                    if (response.getStatusCode() != HttpStatus.CREATED.value()) {
                        log.error("Error uploading file to storage for file [{}]",
                                Utilities.sanitizeString(filename));
                        throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
                                ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                                "Error uploading csv file");
                    }
                    return filename;
                })
                .onErrorMap(BlobStorageException.class, e -> {
                    log.error("Azure Blob Storage upload failed for file {}", filename, e);
                    return new RuntimeException("Error uploading CSV to Blob Storage.", e);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }


    private YearMonth getYearMonth (String yearMonthString){
        return YearMonth.parse(yearMonthString.toLowerCase(), BATCH_MONTH_FORMAT);
    }

    @Override
    public Mono<Void> postponeTransaction(String merchantId, String initiativeId, String rewardBatchId, String transactionId) {

        return findTransactionToPostpone(merchantId, initiativeId, rewardBatchId, transactionId)
                .flatMap(trx -> findCurrentBatchForPostpone(rewardBatchId)
                        .flatMap(currentBatch -> postponeTransactionOnNextBatch(trx, currentBatch, initiativeId)))
                .then();
    }

    private Mono<RewardTransaction> findTransactionToPostpone(String merchantId, String initiativeId, String rewardBatchId, String transactionId) {
        return rewardBatchTransactionReadPort.findTransactionInBatch(initiativeId, merchantId, rewardBatchId, transactionId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(
                        HttpStatus.NOT_FOUND,
                        String.format(ExceptionMessage.TRANSACTION_NOT_FOUND, transactionId)
                )));
    }

    private Mono<RewardBatch> findCurrentBatchForPostpone(String rewardBatchId) {
        return rewardBatchRepository.findById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        ExceptionCode.REWARD_BATCH_NOT_FOUND,
                        String.format(ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH, rewardBatchId)
                )));
    }

    private Mono<RewardTransaction> postponeTransactionOnNextBatch(RewardTransaction trx, RewardBatch currentBatch, String initiativeId) {
        return validateCurrentBatchCanPostpone(currentBatch)
                .flatMap(batch -> {
                    YearMonth nextBatchMonth = getNextBatchMonth(batch);
                    return getInitiativeDataForPostpone(initiativeId)
                            .flatMap(initiativeData -> validatePostponeLimit(initiativeData, nextBatchMonth))
                            .then(Mono.defer(() -> createNextBatchForPostpone(batch, initiativeId, nextBatchMonth)))
                            .flatMap(nextBatch -> moveTransactionToNextBatch(trx, batch, nextBatch, initiativeId));
                });
    }

    private Mono<RewardBatch> validateCurrentBatchCanPostpone(RewardBatch currentBatch) {
        if (currentBatch.getStatus() != RewardBatchStatus.CREATED) {
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                    ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH
            ));
        }
        return Mono.just(currentBatch);
    }

    private YearMonth getNextBatchMonth(RewardBatch currentBatch) {
        YearMonth currentBatchMonth = YearMonth.parse(currentBatch.getMonth());
        YearMonth nextBatchMonth = currentBatchMonth.plusMonths(1);
        log.info("[POSTPONE_TRANSACTION] Current batch month: {}, next batch month: {}", currentBatchMonth, nextBatchMonth);
        return nextBatchMonth;
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

    private Mono<Void> validatePostponeLimit(InitiativeDetailDTO initiativeData, YearMonth nextBatchMonth) {
        YearMonth maxAllowedMonth = YearMonth.from(initiativeData.getFruitionEndDate()).plusMonths(1);

        log.info("[POSTPONE_TRANSACTION] InitiativeEndDate: {}, maxAllowedMonth: {}, nextBatchMonth: {}",
                initiativeData.getFruitionEndDate(), maxAllowedMonth, nextBatchMonth);

        if (nextBatchMonth.isAfter(maxAllowedMonth)) {
            log.warn("[POSTPONE_TRANSACTION] Postpone limit exceeded! nextBatchMonth={} > maxAllowedMonth={}",
                    nextBatchMonth, maxAllowedMonth);
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED,
                    ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED
            ));
        }

        log.info("[POSTPONE_TRANSACTION] Postpone validation passed, creating next batch");
        return Mono.empty();
    }

    private Mono<RewardBatch> createNextBatchForPostpone(RewardBatch currentBatch, String initiativeId, YearMonth nextBatchMonth) {
        return findOrCreateBatch(
                initiativeId,
                currentBatch.getMerchantId(),
                currentBatch.getPosType(),
                nextBatchMonth.toString(),
                currentBatch.getBusinessName()
        ).flatMap(this::validateNextBatchCanReceivePostponedTransaction);
    }

    private Mono<RewardBatch> validateNextBatchCanReceivePostponedTransaction(RewardBatch nextBatch) {
        if (nextBatch.getStatus() != RewardBatchStatus.CREATED) {
            return Mono.error(new ClientExceptionNoBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH
            ));
        }
        return Mono.just(nextBatch);
    }

    private Mono<RewardTransaction> moveTransactionToNextBatch(RewardTransaction trx, RewardBatch currentBatch, RewardBatch nextBatch, String initiativeId) {
        long accruedRewardCents = trx.getRewards().get(initiativeId).getAccruedRewardCents();
        BatchCountersDTO oldBatchCounters = buildOldBatchCounters(trx, accruedRewardCents);
        BatchCountersDTO newBatchCounters = buildNewBatchCounters(trx, accruedRewardCents);

        return rewardBatchRepository.updateTotals(currentBatch.getInitiativeId(), currentBatch.getId(), oldBatchCounters)
                .then(rewardBatchRepository.updateTotals(nextBatch.getInitiativeId(), nextBatch.getId(), newBatchCounters))
                .then(Mono.defer(() -> savePostponedTransaction(trx, nextBatch)));
    }

    private BatchCountersDTO buildOldBatchCounters(RewardTransaction trx, long accruedRewardCents) {
        BatchCountersDTO counters = BatchCountersDTO.newBatch()
                .decrementInitialAmountCents(accruedRewardCents)
                .decrementNumberOfTransactions();

        if (isSuspended(trx)) {
            counters
                    .decrementSuspendedAmountCents(accruedRewardCents)
                    .decrementTrxElaborated()
                    .decrementTrxSuspended();
        }

        return counters;
    }

    private BatchCountersDTO buildNewBatchCounters(RewardTransaction trx, long accruedRewardCents) {
        BatchCountersDTO counters = BatchCountersDTO.newBatch()
                .incrementInitialAmountCents(accruedRewardCents)
                .incrementNumberOfTransactions(1L);

        if (isSuspended(trx)) {
            counters
                    .incrementSuspendedAmountCents(accruedRewardCents)
                    .incrementTrxElaborated()
                    .incrementTrxSuspended();
        }

        return counters;
    }

    private boolean isSuspended(RewardTransaction trx) {
        return RewardBatchTrxStatus.SUSPENDED.equals(trx.getRewardBatchTrxStatus());
    }

    private Mono<RewardTransaction> savePostponedTransaction(RewardTransaction trx, RewardBatch nextBatch) {
        trx.setRewardBatchId(nextBatch.getId());
        trx.setRewardBatchInclusionDate(LocalDateTime.now());
        trx.setUpdateDate(LocalDateTime.now());

        return rewardTransactionRepository.save(trx);
    }

    @Data
    public static class TotalAmount {
        private long total;
    }

    @Override
    public Mono<Void> deleteEmptyRewardBatches() {

        String currentMonth = LocalDate.now()
                .withDayOfMonth(1)
                .toString()
                .substring(0, 7);

        Query toDeleteQuery = Query.query(new Criteria().andOperator(
                Criteria.where(RewardBatch.Fields.numberOfTransactions).in(0L, 0),
                Criteria.where(RewardBatch.Fields.month).lt(currentMonth)
        ));

        return reactiveMongoTemplate.getMongoDatabase()
                .doOnNext(db -> log.info("[CANCEL_EMPTY_BATCHES] DB={}", db.getName()))
                .then(Mono.fromCallable(() -> reactiveMongoTemplate.getCollectionName(RewardBatch.class))
                        .doOnNext(c -> log.info("[CANCEL_EMPTY_BATCHES] Collection={}", c))
                )
                .then(reactiveMongoTemplate.count(new Query(), RewardBatch.class)
                        .doOnNext(total -> log.info("[CANCEL_EMPTY_BATCHES] Total docs={}", total))
                )
                .then(reactiveMongoTemplate.count(toDeleteQuery, RewardBatch.class)
                        .doOnNext(match -> log.info("[CANCEL_EMPTY_BATCHES] Matching docs={}", match))
                )
                .thenMany(reactiveMongoTemplate.find(toDeleteQuery, RewardBatch.class)
                        .doOnNext(rewardBatch -> log.info("[CANCEL_EMPTY_BATCHES] WILL DELETE rewardBatch={}",rewardBatch))
                )
                .concatMap(b ->
                        reactiveMongoTemplate.remove(
                                        Query.query(Criteria.where("_id").is(b.getId())),
                                        RewardBatch.class
                                )
                                .map(DeleteResult::getDeletedCount)
                )
                .reduce(0L, Long::sum)
                .doOnNext(count -> log.info("[CANCEL_EMPTY_BATCHES] Deleted {} empty batches", count))
                .then();
    }

}
