package it.gov.pagopa.idpay.transactions.service;

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
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.stream.Collectors;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {


  private final RewardBatchRepository rewardBatchRepository;
  private final RewardTransactionRepository rewardTransactionRepository;
  private static final Set<String> OPERATORS = Set.of("operator1", "operator2", "operator3");

    //private static final String CSV_HEADER = String.join(";",
    //        "ID", "ID_TRX_ACQUIRER", "ACQUIRER_CODE", "TRX_DATE", "HPAN",
    //        "OPERATION_TYPE", "CIRCUIT_TYPE", "ID_TRX_ISSUER", "CORRELATION_ID",
    //        "AMOUNT_CENTS", "AMOUNT_CURRENCY", "MCC", "ACQUIRER_ID", "MERCHANT_ID",
    //        "POINT_OF_SALE_ID", "TERMINAL_ID", "BIN", "SENDER_CODE", "FISCAL_CODE",
    //        "VAT", "POS_TYPE", "PAR", "STATUS", "REJECTION_REASONS_LIST",
    //        "INITIATIVE_REJECTION_REASONS_MAP", "INITIATIVES_LIST", "REWARDS_MAP_SUMMARY",
    //        "USER_ID", "MASKED_PAN", "BRAND_LOGO", "OPERATION_TYPE_TRANSCODED",
    //        "EFFECTIVE_AMOUNT_CENTS", "TRX_CHARGE_DATE", "REFUND_INFO_SUMMARY",
    //        "ELABORATION_DATE_TIME", "CHANNEL", "ADDITIONAL_PROPERTIES_MAP",
    //        "INVOICE_DATA_SUMMARY", "CREDIT_NOTE_DATA_SUMMARY", "TRX_CODE",
    //        "REWARD_BATCH_ID", "REWARD_BATCH_TRX_STATUS", "REWARD_BATCH_REJECTION_REASON",
    //        "REWARD_BATCH_INCLUSION_DATE", "FRANCHISE_NAME", "POINT_OF_SALE_TYPE",
    //        "BUSINESS_NAME", "INVOICE_UPLOAD_DATE", "SAMPLING_KEY", "UPDATE_DATE",
    //        "EXTENDED_AUTHORIZATION", "VOUCHER_AMOUNT_CENTS"
    //);

    private static final String CSV_HEADER = String.join(";",
            "Data e ora", "Elettrodomestico", "Codice Fiscale Beneficiario", "ID transazione", "Codice sconto",
            "Totale della spesa", "Sconto applicato", "Importo autorizzato", "Numero fattura",
            "Fattura", "Stato"
    );

    public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository,
                                RewardTransactionRepository rewardTransactionRepository) {
    this.rewardBatchRepository = rewardBatchRepository;
    this.rewardTransactionRepository = rewardTransactionRepository;
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
        batch.setStatus(RewardBatchStatus.SENT);
        batch.setUpdateDate(LocalDateTime.now());
        return rewardBatchRepository.save(batch)
            .then();
      });
}


    @Override
    public Mono<RewardBatch> suspendTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request) {
        return rewardBatchRepository.findByIdAndStatus(rewardBatchId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(HttpStatus.NOT_FOUND,
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
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(HttpStatus.NOT_FOUND,
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
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(HttpStatus.NOT_FOUND,
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

private String buildBatchName(YearMonth month) {
  String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
  String year = String.valueOf(month.getYear());

  return String.format("%s %s", monthName, year);
}


    @Override
    public Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.EVALUATING)
                        && rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.BAD_REQUEST,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
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
                                            HttpStatus.BAD_REQUEST,
                                            ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                                            ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE.formatted(rewardBatchId)
                                    ))
                                            : Mono.just(rewardBatch)
                            );
                })
                .map(rewardBatch -> {
                    rewardBatch.setStatus(RewardBatchStatus.APPROVING);
                    rewardBatch.setUpdateDate(LocalDateTime.now());
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
                        HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))

                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.APPROVING)
                        &&  rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3 ))

                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.BAD_REQUEST,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
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
                .map(originalBatch -> {
                    originalBatch.setStatus(RewardBatchStatus.APPROVED);
                    originalBatch.setUpdateDate(LocalDateTime.now());
                    return originalBatch;
                })
                .flatMap(rewardBatchRepository::save)
                .flatMap(savedBatch ->
                        this.generateAndSaveCsv(rewardBatchId, initiativeId)
                                .onErrorResume(e -> {
                                    log.error("Critical error while generating CSV for batch {}", rewardBatchId, e);
                                    return Mono.empty();
                                })
                                .thenReturn(savedBatch)
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


        public Mono<Void> generateAndSaveCsv(String rewardBatchId, String initiativeId) {

            log.info("[GENERATE_AND_SAVE_CSV] Generate CSV for initiative {} and batch {}",
                    Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(rewardBatchId) );

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String filename = "report_" + rewardBatchId + "_" + timestamp + ".csv";

            List<RewardBatchTrxStatus> statusList = new ArrayList<>();
            statusList.add(RewardBatchTrxStatus.APPROVED);
            Flux<RewardTransaction> transactionFlux = rewardTransactionRepository.findByFilter(
                    rewardBatchId, initiativeId, statusList);

            Flux<String> csvRowsFlux = transactionFlux
                    .map(transaction -> this.mapTransactionToCsvRow(transaction, initiativeId));

            Flux<String> fullCsvFlux = Flux.just(CSV_HEADER).concatWith(csvRowsFlux);

            return fullCsvFlux.collect(StringBuilder::new, (sb, s) -> sb.append(s).append("\n"))
                    .map(StringBuilder::toString)
                    .flatMap(csvContent -> {
                        return saveCsvToLocalFile(filename, csvContent);
                    })
                    .doOnTerminate(() -> log.info("CSV generation has been completed for batch: {}", rewardBatchId))
            .then();
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
                    //trx.getRewards().get(initiativeId).getProvidedRewardCents() != null
                    //        ? centsToEuroString.apply(trx.getRewards().get(initiativeId).getProvidedRewardCents())
                    //        : "",
                    safeToString.apply(trx.getInvoiceData().getDocNumber()),
                    safeToString.apply(trx.getInvoiceData().getFilename()),
                    safeToString.apply(trx.getRewardBatchTrxStatus().getDescription())
            );
        }

        private Mono<String> saveCsvToLocalFile(String filename, String csvContent) {
            // 1. Definisci la directory di output (usiamo la temp directory del sistema + una sottocartella)
            //Path outputDirectory = Paths.get(System.getProperty("java.io.tmpdir"), "batch_reports");
            //Path filePath = outputDirectory.resolve(filename);

            // 1. Definisci la directory di output con il percorso Windows fornito
            // Usiamo Paths.get() che gestisce correttamente gli slash su Windows.
            Path outputDirectory = Paths.get("C:", "Users", "EMELIGMWW", "OneDrive - NTT DATA EMEAL", "Desktop", "PagoPA", "CSV - conferma lotto");
            Path filePath = outputDirectory.resolve(filename);

            try {
                Files.createDirectories(outputDirectory);
            } catch (IOException e) {
                log.error("Unable to create output directory: {}", outputDirectory, e);
                return Mono.error(new RuntimeException("Unable to access or create output directory.", e));
            }

            return Mono.fromCallable(() -> {
                Files.writeString(filePath, csvContent, StandardCharsets.UTF_8);
                return filePath.toAbsolutePath().toString();
            }).onErrorMap(IOException.class, e -> {
                log.error("Error writing CSV file to disk: {}", filePath, e);
                return new RuntimeException("I/O error while writing CSV report.", e);
            });
        }

    @Data
    public static class TotalAmount {
        private long total;
    }
}
