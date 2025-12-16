package it.gov.pagopa.idpay.transactions.service;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.dto.DownloadRewardBatchResponseDTO;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import java.time.LocalDate;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {


  private final RewardBatchRepository rewardBatchRepository;
  private final RewardTransactionRepository rewardTransactionRepository;
  private final UserRestClient userRestClient;


    private static final String OPERATOR_1 = "operator1";
    private static final String OPERATOR_2 = "operator2";
    private static final String OPERATOR_3 = "operator3";

    private static final Set<String> OPERATORS = Set.of(OPERATOR_1, OPERATOR_2, OPERATOR_3);
    private final ApprovedRewardBatchBlobService approvedRewardBatchBlobService;

  private static final String CSV_HEADER = String.join(";",
            "Data e ora", "Elettrodomestico", "Codice Fiscale Beneficiario", "ID transazione", "Codice sconto",
            "Totale della spesa", "Sconto applicato",//"Importo autorizzato",
            "Numero fattura",
            "Fattura", "Stato"
    );

  private static final String REWARD_BATCHES_PATH_STORAGE_FORMAT = "initiative/%s/merchant/%s/batch/%s/";
  private static final String REWARD_BATCHES_REPORT_NAME_FORMAT = "%s_%s_%s.csv";

  public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository, RewardTransactionRepository rewardTransactionRepository, UserRestClient userRestClient, ApprovedRewardBatchBlobService approvedRewardBatchBlobService) {
    this.rewardBatchRepository = rewardBatchRepository;
    this.rewardTransactionRepository = rewardTransactionRepository;
      this.userRestClient = userRestClient;
      this.approvedRewardBatchBlobService = approvedRewardBatchBlobService;
  }

  @Override
  public Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, String month, String businessName) {
    return rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(merchantId, posType,
            month)
        .switchIfEmpty(Mono.defer(() ->
            createBatch(merchantId, posType, month, businessName)
                .onErrorResume(DuplicateKeyException.class, ex ->
                    rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(merchantId,
                        posType, month))));
  }

  @Override
  public Mono<Page<RewardBatch>> getRewardBatches(String merchantId, String organizationRole, String status, String assigneeLevel, Pageable pageable) {
    boolean callerIsOperator = isOperator(organizationRole);

    return rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, callerIsOperator, pageable)
        .collectList()
        .zipWith(rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, callerIsOperator))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }

  private boolean isOperator(String role) {
    return role != null && OPERATORS.contains(role.toLowerCase());
  }

  private Mono<RewardBatch> createBatch(String merchantId, PosType posType, String month, String businessName) {

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
        .initialAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .reportPath(null)
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactionsSuspended(0L)
        .numberOfTransactionsRejected(0L)
        .creationDate(LocalDateTime.now())
        .updateDate(LocalDateTime.now())
        .build();

    return rewardBatchRepository.save(batch);
  }

  @Override
  public Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents) {
    return rewardBatchRepository.incrementTotals(batchId, accruedAmountCents);
  }

  public Mono<RewardBatch> decrementTotals(String batchId, long accruedAmountCents) {
    return rewardBatchRepository.decrementTotals(batchId, accruedAmountCents);
  }

  @Override
  public Mono<Void> sendRewardBatch(String merchantId, String batchId) {
    return rewardBatchRepository.findById(batchId)
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

          return noPreviousBatchesInCreatedStatus(merchantId, batchMonth, batch.getPosType())
              .flatMap(allPreviousSent -> {
                if (Boolean.FALSE.equals(allPreviousSent)) {
                  log.warn("[SEND_REWARD_BATCHES] Previous batches of type {} not sent yet for merchant {}!",
                      batch.getPosType(), Utilities.sanitizeString(merchantId));
                  return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                      ExceptionConstants.ExceptionCode.REWARD_BATCH_PREVIOUS_NOT_SENT));
                }

                batch.setStatus(RewardBatchStatus.SENT);
                batch.setUpdateDate(LocalDateTime.now());
                return rewardBatchRepository.save(batch);
              })
              .then();
        });
  }

  private Mono<Boolean> noPreviousBatchesInCreatedStatus(String merchantId, YearMonth currentMonth, PosType posType) {
    return rewardBatchRepository.findByMerchantIdAndPosType(merchantId, posType)
        .filter(batch -> {
          YearMonth batchMonth = YearMonth.parse(batch.getMonth());
          return batchMonth.isBefore(currentMonth);
        })
        .filter(batch -> batch.getStatus() == RewardBatchStatus.CREATED)
        .hasElements()
        .map(hasCreated -> !hasCreated);
  }

    @Override
    public Mono<RewardBatch> suspendTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request) {
        return rewardBatchRepository.findByIdAndStatus(rewardBatchId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds()))
                .flatMap(trxId -> rewardTransactionRepository
                        .updateStatusAndReturnOld(rewardBatchId, trxId, RewardBatchTrxStatus.SUSPENDED, request.getReason())
                )
                .reduce(new BatchCountersDTO(0L, 0L, 0L, 0L), (acc, trxOld) -> {

                    if (trxOld == null) {
                        return acc;
                    }

                    Long accrued = trxOld.getRewards().get(initiativeId) != null
                            ? trxOld.getRewards().get(initiativeId).getAccruedRewardCents()
                            : null;

                    switch (trxOld.getRewardBatchTrxStatus()) {

                        case RewardBatchTrxStatus.SUSPENDED -> log.info("Skipping  handler  for transaction  {}:  status  is already  SUSPENDED",  trxOld.getId());

                        case RewardBatchTrxStatus.APPROVED -> {
                            acc.incrementTrxSuspended();

                            if (accrued != null) {
                                acc.decrementTotalApprovedAmountCents(accrued);
                            }
                        }

                        case RewardBatchTrxStatus.TO_CHECK,
                             RewardBatchTrxStatus.CONSULTABLE -> {
                            acc.incrementTrxElaborated();
                            acc.incrementTrxSuspended();

                            if (accrued != null) {
                                acc.decrementTotalApprovedAmountCents(accrued);
                            }
                        }

                        case RewardBatchTrxStatus.REJECTED -> {
                            acc.decrementTrxRejected();
                            acc.incrementTrxSuspended();
                        }
                    }

                    return acc;
                })
                .flatMap(acc ->
                        rewardBatchRepository.updateTotals(
                                rewardBatchId,
                                acc.getTrxElaborated(),
                                acc.getTotalApprovedAmountCents(),
                                acc.getTrxRejected(),
                                acc.getTrxSuspended()
                        )
                );
    }

    @Override
    public Mono<RewardBatch> rejectTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request) {
        return rewardBatchRepository.findByIdAndStatus(rewardBatchId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds()))
                .flatMap(trxId -> rewardTransactionRepository
                        .updateStatusAndReturnOld(rewardBatchId, trxId, RewardBatchTrxStatus.REJECTED, request.getReason())
                )
                .reduce(new BatchCountersDTO(0L, 0L, 0L, 0L),
                        (acc, trxOld) -> {

                    if (trxOld == null) {
                        return acc;
                    }

                    Long accrued = trxOld.getRewards().get(initiativeId) != null
                            ? trxOld.getRewards().get(initiativeId).getAccruedRewardCents()
                            : null;

                    switch (trxOld.getRewardBatchTrxStatus()) {

                        case RewardBatchTrxStatus.REJECTED ->
                                log.info("Skipping  handler  for transaction  {}:  status  is already  REJECTED",  trxOld.getId());

                        case RewardBatchTrxStatus.APPROVED -> {
                            acc.incrementTrxRejected();

                            if (accrued != null) {
                                acc.decrementTotalApprovedAmountCents(accrued);
                            }
                        }

                            case RewardBatchTrxStatus.TO_CHECK,
                                 RewardBatchTrxStatus.CONSULTABLE -> {
                                acc.incrementTrxElaborated();
                                acc.incrementTrxRejected();

                                if (accrued != null) {
                                    acc.decrementTotalApprovedAmountCents(accrued);
                                }
                            }

                            case RewardBatchTrxStatus.SUSPENDED -> {
                                acc.decrementTrxSuspended();
                                acc.incrementTrxRejected();
                            }

                    }

                    return acc;
                })
                .flatMap(acc ->
                        rewardBatchRepository.updateTotals(
                                rewardBatchId,
                                acc.getTrxElaborated(),
                                acc.getTotalApprovedAmountCents(),
                                acc.getTrxRejected(),
                                acc.getTrxSuspended()
                        )
                );
    }


    @Override
    public Mono<RewardBatch> approvedTransactions(String rewardBatchId, TransactionsRequest request, String initiativeId) {
        return rewardBatchRepository.findByIdAndStatus(rewardBatchId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds()))
                .flatMap(trxId -> rewardTransactionRepository.updateStatusAndReturnOld(rewardBatchId, trxId, RewardBatchTrxStatus.APPROVED, null))
                .reduce(new BatchCountersDTO(0L, 0L, 0L, 0L), (acc, trxOld) -> {
                    switch (trxOld.getRewardBatchTrxStatus()){
                        case RewardBatchTrxStatus.APPROVED -> log.info("Skipping  handler  for transaction  {}:  status  is already  APPROVED",  trxOld.getId());
                        case RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE -> acc.incrementTrxElaborated();
                        case RewardBatchTrxStatus.SUSPENDED -> {
                            acc.decrementTrxSuspended();
                            if(trxOld.getRewards().get(initiativeId) != null && trxOld.getRewards().get(initiativeId).getAccruedRewardCents() != null) {
                                acc.incrementTotalApprovedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                            }
                        }
                        case RewardBatchTrxStatus.REJECTED -> {
                            acc.decrementTrxRejected();
                            if(trxOld.getRewards().get(initiativeId) != null && trxOld.getRewards().get(initiativeId).getAccruedRewardCents() != null) {
                                acc.incrementTotalApprovedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                            }
                        }
                    }
                    return acc;
                })

                .flatMap(acc -> rewardBatchRepository.updateTotals(rewardBatchId, acc.getTrxElaborated(), acc.getTotalApprovedAmountCents(), acc.getTrxRejected(), acc.getTrxSuspended()));
    }
    @Scheduled (cron = "${app.transactions.reward-batch.to-evaluating.schedule}")
    void evaluatingRewardBatchStatusScheduler(){
        log.info("[EVALUATING_REWARD_BATCH][SCHEDULER] Start to evaluating all reward batches with status SENT");
        evaluatingRewardBatches(null)
                .onErrorResume(RewardBatchNotFound.class, x -> {
                    log.error("[EVALUATING_REWARD_BATCH][SCHEDULER] " + ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT, x);
                    return Mono.just(0L);
                })
                .subscribe(numberUpdateBatch -> log.info("[EVALUATING_REWARD_BATCH][SCHEDULER] Completed evaluation. Updated {} reward batches to status EVALUATING", numberUpdateBatch));
    }

    @Override
    public Mono<Long> evaluatingRewardBatches(List<String> rewardBatchesRequest) {
        log.info("[EVALUATING_REWARD_BATCH] Starting evaluation of reward batches with status SENT");
        Flux<RewardBatch> rewardBatchToElaborate;
        if (rewardBatchesRequest == null) {
            rewardBatchToElaborate = rewardBatchRepository.findByStatus(RewardBatchStatus.SENT);
        } else {
            rewardBatchToElaborate = Flux.fromIterable(rewardBatchesRequest)
                    .flatMap(batchId -> rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT));
        }

        return rewardBatchToElaborate
                .flatMap(rewardBatch -> {
                    log.info("[EVALUATING_REWARD_BATCH] Evaluating reward batch {}", Utilities.sanitizeString(rewardBatch.getId()));
                    return rewardTransactionRepository.rewardTransactionsByBatchId(rewardBatch.getId())
                            .thenReturn(rewardBatch)
                            .log("[EVALUATING_REWARD_BATCH]Completed evaluation of transactions for reward batch %s".formatted(Utilities.sanitizeString(rewardBatch.getId())));
                })
                .flatMap(batch -> rewardBatchRepository.updateStatusAndApprovedAmountCents(batch.getId(), RewardBatchStatus.EVALUATING, batch.getInitialAmountCents())
                        .log("[EVALUATING_REWARD_BATCH] Reward batch %s moved to status EVALUATING".formatted(Utilities.sanitizeString(batch.getId()))))
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
                        ? rewardBatchRepository.findById(rewardBatchId)
                        : rewardBatchRepository.findByMerchantIdAndId(merchantId, rewardBatchId);

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

                    if (!RewardBatchStatus.APPROVED.equals(batch.getStatus())) {
                        throw new RewardBatchNotApprovedException(
                                REWARD_BATCH_NOT_APPROVED,
                                ERROR_MESSAGE_REWARD_BATCH_NOT_APPROVED.formatted(rewardBatchId)
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

    private String buildBatchName(YearMonth month) {
  String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
  String year = String.valueOf(month.getYear());

  return String.format("%s %s", monthName, year);
}


    @Override
    public Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
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
                    Flux<RewardBatch> previousBatchesFlux = rewardBatchRepository.findRewardBatchByMonthBefore(
                            rewardBatch.getMerchantId(),
                            rewardBatch.getPosType(),
                            rewardBatch.getMonth()
                    );
                    Mono<Boolean> hasUnapprovedBatch = previousBatchesFlux
                            .filter(batch -> !batch.getStatus().equals(RewardBatchStatus.APPROVED))
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
                .flatMap(rewardBatchRepository::save);
    }



    @Override
    public Mono<Void> rewardBatchConfirmationBatch(String initiativeId, List<String> rewardBatchIds) {
        if (rewardBatchIds != null && !rewardBatchIds.isEmpty()) {
            return Flux.fromIterable(rewardBatchIds)
                    .doOnNext(rewardBatchId ->
                            log.info("Processing batch {}", rewardBatchId)
                    )
                    .flatMap(rewardBatchId ->
                            this.processSingleBatchSafe(rewardBatchId, initiativeId)
                    )

                    .then();

        } else {
            return rewardBatchRepository.findRewardBatchByStatus(RewardBatchStatus.APPROVING)
                    .collectList()
                    .flatMapMany(batchList -> {
                        if (batchList.isEmpty()) {
                            log.warn("No batches found with status APPROVING to process.");
                            return Flux.empty();
                        } else {
                            log.info("Found {} batches with status APPROVING to process.", batchList.size());
                            return Flux.fromIterable(batchList);
                        }
                    })
                    .doOnNext(rewardBatch ->
                            log.info("Processing batch {}",
                                    rewardBatch.getId())
                    )
                    .flatMap(rewardBatch -> {
                        String rewardBatchId = rewardBatch.getId();
                        return processSingleBatchSafe(rewardBatchId, initiativeId);
                    })
                    .then();
        }
    }


    public Mono<RewardBatch> processSingleBatchSafe(String rewardBatchId, String initiativeId) {
        return this.processSingleBatch(rewardBatchId, initiativeId)
                .onErrorResume(ClientExceptionWithBody.class, error -> {
                     log.warn(error.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<RewardBatch> processSingleBatch(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))

                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.APPROVING)
                        &&  rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3 ))

                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(originalBatch -> {
                    Mono<Void> transactionsUpdate = updateAndSaveRewardTransactionsToApprove(rewardBatchId, initiativeId);
                    return transactionsUpdate.thenReturn(originalBatch);
                })
                .flatMap(originalBatch -> {
                    if (originalBatch.getNumberOfTransactionsSuspended() != null && originalBatch.getNumberOfTransactionsSuspended() > 0) {
                        Mono<RewardBatch> newBatchMono = createRewardBatchAndSave(originalBatch);
                        Mono<Void> updateTrxMono = newBatchMono
                                .flatMap(newBatch ->
                                        updateAndSaveRewardTransactionsSuspended(rewardBatchId, initiativeId, newBatch.getId())
                                ).then();
                        return updateTrxMono.thenReturn(originalBatch);

                    } else {
                        log.info("numberOfTransactionSuspended = 0 for batch {}", originalBatch.getId());
                        return Mono.just(originalBatch);
                    }
                })
                .flatMap(originalBatch -> {
                            originalBatch.setStatus(RewardBatchStatus.APPROVED);
                            originalBatch.setUpdateDate(LocalDateTime.now());
                            return rewardBatchRepository.save(originalBatch);
                })
                .flatMap(savedBatch ->
                        this.generateAndSaveCsv(rewardBatchId, initiativeId, savedBatch.getMerchantId())
                                .onErrorResume(e -> {
                                    log.error("Critical error while generating CSV for batch {}", Utilities.sanitizeString(rewardBatchId), e);
                                    return Mono.empty();
                                })
                                .flatMap(filename -> {
                                    savedBatch.setFilename(filename);
                                    log.info("Updated batch {} with filename: {}", Utilities.sanitizeString(rewardBatchId), filename);
                                    return rewardBatchRepository.save(savedBatch);
                                })
                );
    }

  Mono<RewardBatch> createRewardBatchAndSave(RewardBatch savedBatch) {

      Mono<RewardBatch> existingBatchMono = rewardBatchRepository.findRewardBatchByFilter(
              null,
              savedBatch.getMerchantId(),
              savedBatch.getPosType(),
              addOneMonth(savedBatch.getMonth()))
              .doOnNext(existingBatch ->
                              log.info("Batch for {} and merchantId {} already exists, with rewardBatchId = {}",
                                      addOneMonthToItalian(savedBatch.getName()),
                                      Utilities.sanitizeString(existingBatch.getMerchantId()),
                                      Utilities.sanitizeString(existingBatch.getId())));

      Mono<RewardBatch> newBatchCreationMono = Mono.just(savedBatch)
              .map(batch -> RewardBatch.builder()
                .id(null)
                .merchantId(savedBatch.getMerchantId())
                .businessName(savedBatch.getBusinessName())
                .month(addOneMonth(savedBatch.getMonth()))
                .posType(savedBatch.getPosType())
                .status(RewardBatchStatus.CREATED)
                .partial(savedBatch.getPartial())
                .name(addOneMonthToItalian(savedBatch.getName()))
                .startDate(savedBatch.getStartDate())
                .endDate(savedBatch.getEndDate())
                .approvedAmountCents(0L)
                .initialAmountCents(0L)
                .numberOfTransactions(savedBatch.getNumberOfTransactionsSuspended())
                .numberOfTransactionsElaborated(0L)
                .numberOfTransactionsSuspended(savedBatch.getNumberOfTransactionsSuspended())
                .numberOfTransactionsRejected(0L)
                .reportPath(savedBatch.getReportPath())
                .assigneeLevel(RewardBatchAssignee.L1)
                .creationDate(LocalDateTime.now())
                .updateDate(LocalDateTime.now())
                .build()
              )
              .flatMap(rewardBatchRepository::save)
              .doOnNext(newBatch ->
                      log.info("Created new batch for {} with rewardBatchId = {}",
                              addOneMonthToItalian(savedBatch.getName()),
                              Utilities.sanitizeString(newBatch.getId()))
              );

      return existingBatchMono
              .switchIfEmpty(newBatchCreationMono)
              .map(foundOrNewBatch -> foundOrNewBatch);

  }

    public String addOneMonth(String yearMonthString) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
        YearMonth yearMonth = YearMonth.parse(yearMonthString, inputFormatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(inputFormatter);
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


        return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
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

    public  Mono<Void> updateAndSaveRewardTransactionsSuspended(String oldBatchId, String initiativeId, String newBatchId) {
        List<RewardBatchTrxStatus> statusList = new ArrayList<>();
        statusList.add(RewardBatchTrxStatus.SUSPENDED);

        return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("No suspended transactions found for the batch {}",  Utilities.sanitizeString(oldBatchId));
                    return Flux.empty();
                }))
                .collectList()
                .doOnNext(list -> {
                    if (!list.isEmpty()) {
                        log.info("Found {} transactions SUSPENDED for batch {}",
                                list.size(),
                                Utilities.sanitizeString(oldBatchId));
                    }
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchId(newBatchId);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then();

    }

    @Override
    public Mono<RewardBatch> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findById(rewardBatchId)
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

                        if (total == 0 || elaborated < Math.ceil(total * 0.15)) {
                            return Mono.error(new BatchNotElaborated15PercentException(
                                    BATCH_NOT_ELABORATED_15_PERCENT,
                                    ERROR_MESSAGE_BATCH_NOT_ELABORATED_15_PERCENT
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L2);
                        return rewardBatchRepository.save(batch);
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

        return rewardBatchRepository.findById(rewardBatchId)
                .flatMap(batch -> {

                    String pathPrefix = String.format(REWARD_BATCHES_PATH_STORAGE_FORMAT,
                            Utilities.sanitizeString(initiativeId),
                            Utilities.sanitizeString(batch.getMerchantId()),
                            Utilities.sanitizeString(rewardBatchId));

                    String reportFilename = String.format(REWARD_BATCHES_REPORT_NAME_FORMAT,
                            batch.getBusinessName(),
                            batch.getName(),
                            batch.getPosType()).trim();
                    String filename = pathPrefix + reportFilename;

                    Flux<RewardTransaction> transactionFlux = rewardTransactionRepository.findByFilter(
                            rewardBatchId, initiativeId, List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED));

                    Flux<String> csvRowsFlux = transactionFlux
                            .flatMap(transaction -> {
                                if(transaction.getFiscalCode() == null || transaction.getFiscalCode().isEmpty()){
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
                            .doOnTerminate(() -> log.info("CSV generation has been completed for batch: {}", Utilities.sanitizeString(rewardBatchId)))
                            .map(uploadedKey -> reportFilename);
                });
    }

        private String mapTransactionToCsvRow(RewardTransaction trx, String initiativeId) {
            Function<Object, String> safeToString = obj -> obj != null ? obj.toString().replace(";", ",") : "";
            Function<LocalDateTime, String> safeDateToString = date ->
                    date != null ? date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : "";

            LongFunction<String> centsToEuroString = cents -> {
                NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ITALY);
                numberFormat.setMinimumFractionDigits(2);
                numberFormat.setMaximumFractionDigits(2);
                return  numberFormat.format(cents / 100.0);

            };

            String productName = trx.getAdditionalProperties().get("productName") != null ? trx.getAdditionalProperties().get("productName") : "";
            String productGtin = trx.getAdditionalProperties().get("productGtin") != null ? trx.getAdditionalProperties().get("productGtin") : "";

            String additionalProperties = productName + "\n" + productGtin;

            String quotedAdditionalProperties = "\"" + additionalProperties + "\"";

            return String.join(";",
                    safeDateToString.apply(trx.getTrxChargeDate()),
                    safeToString.apply(quotedAdditionalProperties),
                    safeToString.apply(trx.getFiscalCode()),
                    safeToString.apply(trx.getId()),
                    safeToString.apply(trx.getTrxCode()),
                    trx.getEffectiveAmountCents() != null ? centsToEuroString.apply(trx.getEffectiveAmountCents()) : "",
                    trx.getRewards().get(initiativeId).getAccruedRewardCents() != null
                            ? centsToEuroString.apply(trx.getRewards().get(initiativeId).getAccruedRewardCents())
                            : "",
                    safeToString.apply(trx.getInvoiceData().getDocNumber()),
                    safeToString.apply(trx.getInvoiceData().getFilename()),
                    safeToString.apply(trx.getRewardBatchTrxStatus().getDescription())
            );
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



    @Override
    public Mono<Void> postponeTransaction(String merchantId, String initiativeId, String rewardBatchId, String transactionId, LocalDate initiativeEndDate) {

      return rewardTransactionRepository.findTransactionInBatch(merchantId, rewardBatchId, transactionId)
          .switchIfEmpty(Mono.error(new ClientExceptionNoBody(
              HttpStatus.NOT_FOUND,
              String.format(ExceptionMessage.TRANSACTION_NOT_FOUND, transactionId)
          )))
          .flatMap(trx -> {

            long accruedRewardCents = trx.getRewards().get(initiativeId).getAccruedRewardCents();

            return rewardBatchRepository.findById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                    HttpStatus.NOT_FOUND,
                    ExceptionCode.REWARD_BATCH_NOT_FOUND,
                    String.format(ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH, rewardBatchId)
                )))
                .flatMap(currentBatch -> {

                  if (currentBatch.getStatus() != RewardBatchStatus.CREATED) {
                    return Mono.error(new ClientExceptionWithBody(
                        HttpStatus.BAD_REQUEST,
                        ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                        ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH
                    ));
                  }

                  YearMonth currentBatchMonth = YearMonth.parse(currentBatch.getMonth());
                  YearMonth nextBatchMonth = currentBatchMonth.plusMonths(1);

                  YearMonth maxAllowedMonth = YearMonth.from(initiativeEndDate).plusMonths(1);

                  if (nextBatchMonth.isAfter(maxAllowedMonth)) {
                    return Mono.error(new ClientExceptionWithBody(
                        HttpStatus.BAD_REQUEST,
                        ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED,
                        ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED
                    ));
                  }

                  return this.findOrCreateBatch(
                          currentBatch.getMerchantId(),
                          currentBatch.getPosType(),
                          nextBatchMonth.toString(),
                          currentBatch.getBusinessName()
                      )
                      .flatMap(nextBatch -> {

                        if (nextBatch.getStatus() != RewardBatchStatus.CREATED) {
                          return Mono.error(new ClientExceptionNoBody(
                              HttpStatus.BAD_REQUEST,
                              ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH
                          ));
                        }

                        return decrementTotals(currentBatch.getId(), accruedRewardCents)
                            .then(incrementTotals(nextBatch.getId(), accruedRewardCents))
                            .then(Mono.defer(() -> {

                              trx.setRewardBatchId(nextBatch.getId());
                              trx.setRewardBatchInclusionDate(LocalDateTime.now());
                              trx.setUpdateDate(LocalDateTime.now());

                              return rewardTransactionRepository.save(trx);
                            }));
                      });
                });
          })
          .then();
    }

  @Data
    public static class TotalAmount {
        private long total;
    }
}
