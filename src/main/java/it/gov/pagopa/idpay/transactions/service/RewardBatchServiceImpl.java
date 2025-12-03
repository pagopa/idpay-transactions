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
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {


  private final RewardBatchRepository rewardBatchRepository;
  private final RewardTransactionRepository rewardTransactionRepository;
  private static final Set<String> OPERATORS = Set.of("operator1", "operator2", "operator3");


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
        Flux<RewardBatch> rewardBatchToElaborate;
        if (rewardBatchesRequest == null) {
            rewardBatchToElaborate = rewardBatchRepository.findByStatus(RewardBatchStatus.SENT);
        } else {
            rewardBatchToElaborate = Flux.fromIterable(rewardBatchesRequest)
                    .flatMap(batchId -> rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT));
        }

        return rewardBatchToElaborate
                .switchIfEmpty(Mono.error(new RewardBatchNotFound(REWARD_BATCH_NOT_FOUND, ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT)))
                .flatMap(rewardBatch -> {
                    log.info("[EVALUATING_REWARD_BATCH] Start to evaluating reward batch {}", Utilities.sanitizeString(rewardBatch.getId()));
                    return rewardTransactionRepository.rewardTransactionsByBatchId(rewardBatch.getId())
                        .thenReturn(rewardBatch);})
                .flatMap(batch -> rewardBatchRepository.updateStatusAndApprovedAmountCents(batch.getId(), RewardBatchStatus.EVALUATING, batch.getInitialAmountCents()))
                .count();
    }

    private String buildBatchName(YearMonth month) {
        String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
        String year = String.valueOf(month.getYear());

        return String.format("%s %s", monthName, year);
    }


    @Override
    public Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId)      {
    return rewardBatchRepository.findRewardBatchById(rewardBatchId)
    .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
            HttpStatus.NOT_FOUND,
            REWARD_BATCH_NOT_FOUND,
            ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
    .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.EVALUATING)
                            &&  !rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3 ))
            .map(rewardBatch -> {
              rewardBatch.setStatus(RewardBatchStatus.APPROVED);
              rewardBatch.setUpdateDate(LocalDateTime.now());
              return rewardBatch;
            })
            .flatMap(rewardBatchRepository::save)
            .flatMap(savedBatch -> {
                Mono<Void> transactionsUpdate = updateAndSaveRewardTransactionsToApprove(rewardBatchId, initiativeId);
                return transactionsUpdate.thenReturn(savedBatch);

            })
            .flatMap(savedBatch -> {
                if (savedBatch.getNumberOfTransactionsSuspended() != null && savedBatch.getNumberOfTransactionsSuspended() > 0) {
                    return createRewardBatchAndSave(savedBatch)
                            .flatMap(newBatch -> updateAndSaveRewardTransactionsSuspended(rewardBatchId, initiativeId, newBatch.getId()).thenReturn(newBatch));
                } else {
                    return Mono.just(savedBatch);
                }
            }).switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                    ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
            )));
  }
  Mono<RewardBatch> createRewardBatchAndSave(RewardBatch savedBatch) {


      Mono<RewardBatch> existingBatchMono = rewardBatchRepository.findRewardBatchByFilter(
              null,
              savedBatch.getMerchantId(),
              savedBatch.getPosType() != null ? savedBatch.getPosType().toString() : null,
              addOneMonth(savedBatch.getMonth()))
              .doOnNext(existingBatch ->
                              log.info("Batch for {} already exists, with rewardBatchId = {}",
                                      addOneMonthToItalian(savedBatch.getName()),
                                      existingBatch.getId()));

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
                              newBatch.getId())
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
             .flatMap(rewardTransaction -> {
               rewardTransaction.setStatus(RewardBatchTrxStatus.APPROVED.toString());
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
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchId(newBatchId);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then();

    }

    @Override
    public Mono<Void> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND
                )))
                .flatMap(batch -> {

                    RewardBatchAssignee assignee = batch.getAssigneeLevel();

                    if (assignee == RewardBatchAssignee.L1) {

                        if (!"operator1".equals(organizationRole)) {
                            return Mono.error(new ResponseStatusException(
                                    HttpStatus.FORBIDDEN,
                                    ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED_FOR_L1_PROMOTION
                            ));
                        }

                        long total = batch.getNumberOfTransactions();
                        long elaborated = batch.getNumberOfTransactionsElaborated();

                        if (total == 0 || elaborated < Math.ceil(total * 0.15)) {
                            return Mono.error(new ResponseStatusException(
                                    HttpStatus.BAD_REQUEST,
                                    ExceptionConstants.ExceptionCode.BATCH_NOT_ELABORATED_15_PERCENT
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L2);
                        return rewardBatchRepository.save(batch).then();
                    }

                    if (assignee == RewardBatchAssignee.L2) {

                        if (!"operator2".equals(organizationRole)) {
                            return Mono.error(new ResponseStatusException(
                                    HttpStatus.FORBIDDEN,
                                    ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED_FOR_L2_PROMOTION
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L3);
                        return rewardBatchRepository.save(batch).then();
                    }

                    return Mono.error(new ResponseStatusException(
                            HttpStatus.BAD_REQUEST,
                            ExceptionConstants.ExceptionCode.INVALID_BATCH_STATE_FOR_PROMOTION
                    ));
                });
    }

    @Data
    public static class TotalAmount {
        private long total;
    }
}
