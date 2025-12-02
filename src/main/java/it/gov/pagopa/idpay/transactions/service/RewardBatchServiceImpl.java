package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
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
import org.springframework.stereotype.Service;
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
            ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND,
            ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
    .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.EVALUATING)
                            &&  rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3 ))
            .map(rewardBatch -> {
              rewardBatch.setStatus(RewardBatchStatus.APPROVING);
              rewardBatch.setUpdateDate(LocalDateTime.now());
              return rewardBatch;
            })
            .flatMap(rewardBatchRepository::save)
            .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                    ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
            )));
  }



    @Override
    public Mono<Void> rewardBatchConfirmationBatch(String initiativeId, List<String> rewardBatchIds) {

        // 1. Definisci il flusso di elaborazione per una lista di ID
        if (rewardBatchIds != null && !rewardBatchIds.isEmpty()) {

            // Converti la lista in Flux, elabora ogni ID e ignora il risultato finale con .then()
            return Flux.fromIterable(rewardBatchIds)

                    // 1. Usa doOnNext per la logica collaterale (logging)
                    .doOnNext(rewardBatchId ->
                            log.info("Processing batch {}", rewardBatchId)
                    )

                    // 2. flatMap per l'elaborazione asincrona
                    .flatMap(rewardBatchId ->
                            this.processSingleBatchSafe(rewardBatchId, initiativeId)
                    )

                    .then(); // Converte Flux<RewardBatch> in Mono<Void>

        } else {

// Trova tutti i batch
            return rewardBatchRepository.findRewardBatchByStatus(RewardBatchStatus.APPROVING)

                    // 1. Converti il Flux<RewardBatch> in un Mono<List<RewardBatch>>
                    .collectList()

                    // 2. Verifica la lista (Gestione NOT_FOUND, che ora include il log condizionale) e riconverti in Flux
                    .flatMapMany(batchList -> {
                        if (batchList.isEmpty()) {

                            // ✅ LOG 1: Logga solo l'assenza di batch e poi lancia l'errore
                            log.warn("No batches found with status APPROVING to process.");

                            // Restituisci Flux.empty() per completare il flusso
                            return Flux.empty();
                        } else {

                            // ✅ LOG 2: Logga il numero totale solo se la lista NON è vuota
                            log.info("Found {} batches with status APPROVING to process.", batchList.size());

                            // Se la lista ha elementi, restituisci un Flux<RewardBatch> per continuare
                            return Flux.fromIterable(batchList);
                        }
                    })

                    // ✅ LOG 3: Logga il singolo batch che sta per essere processato
                    .doOnNext(rewardBatch ->
                            log.info("Processing batch {}",
                                    rewardBatch.getId())
                    )

                    // 3. CONTINUA QUI con i tuoi operatori originali che lavorano su singoli RewardBatch
                    .flatMap(rewardBatch -> {
                        String rewardBatchId = rewardBatch.getId();
                        return processSingleBatchSafe(rewardBatchId, initiativeId);
                    })
                    .then(); // Per restituire Mono<Void>
        }
    }


    public Mono<RewardBatch> processSingleBatchSafe(String rewardBatchId, String initiativeId) {

        // Chiama la logica originale
        return this.processSingleBatch(rewardBatchId, initiativeId)

                // 2. Intercetta TUTTI gli errori lanciati (sia NOT_FOUND che INVALID_STATE)
                .onErrorResume(ClientExceptionWithBody.class, error -> {

                    // 3. (OPZIONALE) Qui puoi loggare l'errore prima di saltarlo
                     log.warn(error.getMessage());

                    // 4. Trasforma l'errore in un segnale di completamento (Mono.empty())
                    // Questo permette al Flux di continuare con il prossimo ID.
                    return Mono.empty();
                });
    }

    private Mono<RewardBatch> processSingleBatch(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                // 🛑 GESTIONE NOT_FOUND e INVALID STATE... (lasciati invariati)
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

                // 1. Esegui l'aggiornamento delle transazioni approvate
                .flatMap(originalBatch -> {
                    Mono<Void> transactionsUpdate = updateAndSaveRewardTransactionsToApprove(rewardBatchId, initiativeId);
                    // Propaga l'oggetto originalBatch, non ancora salvato come APPROVED
                    return transactionsUpdate.thenReturn(originalBatch);
                })

                // 2. Logica Sospensioni
                .flatMap(originalBatch -> {
                    if (originalBatch.getNumberOfTransactionsSuspended() != null && originalBatch.getNumberOfTransactionsSuspended() > 0) {

                        // A. Crea e salva il nuovo batch (newBatchMono)
                        Mono<RewardBatch> newBatchMono = createRewardBatchAndSave(originalBatch);

                        // B. Esegui l'aggiornamento delle transazioni sospese (Mono<Void>)
                        Mono<Void> updateTrxMono = newBatchMono
                                .flatMap(newBatch ->
                                        updateAndSaveRewardTransactionsSuspended(rewardBatchId, initiativeId, newBatch.getId())
                                ).then(); // Trasforma il risultato in Mono<Void>

                        // C. Attendi che l'aggiornamento sia finito, ma restituisci il batch ORIGINALE
                        // Questo è cruciale: il flusso continua con originalBatch.
                        return updateTrxMono.thenReturn(originalBatch);

                    } else {
                        // Se non ci sono sospesi, restituisci semplicemente il batch ORIGINALE
                        return Mono.just(originalBatch);
                    }
                })

                // 3. AGGIORNAMENTO DI STATO FINALE (su originalBatch)
                .map(originalBatch -> {
                    // Modifica lo stato sull'oggetto originale che è passato attraverso tutta la catena
                    originalBatch.setStatus(RewardBatchStatus.APPROVED);
                    originalBatch.setUpdateDate(LocalDateTime.now());
                    return originalBatch;
                })

                // 4. SALVATAGGIO FINALE (su originalBatch)
                .flatMap(rewardBatchRepository::save);
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
                // 1. Contiamo gli elementi del Flux
                .collectList() // Trasforma Flux<RewardTransaction> in Mono<List<RewardTransaction>>

                // 2. Esegui la logica di logging
                .doOnNext(list -> {
                    log.info("Found {} transactions to approve for batch {}",
                            list.size(),
                            oldBatchId);
                })

                // 3. Riconvertiamo la List in Flux per l'elaborazione (flatMap)
                .flatMapMany(Flux::fromIterable)

                // 4. Continua l'elaborazione elemento per elemento (flatMap originale)
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then(); // Segnale di completamento

    }

    public  Mono<Void> updateAndSaveRewardTransactionsSuspended(String oldBatchId, String initiativeId, String newBatchId) {
        List<RewardBatchTrxStatus> statusList = new ArrayList<>();
        statusList.add(RewardBatchTrxStatus.SUSPENDED);

        return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("No suspended transactions found for the batch {}",  Utilities.sanitizeString(oldBatchId));
                    return Flux.empty();
                }))
                .collectList() // Trasforma Flux<RewardTransaction> in Mono<List<RewardTransaction>>

// ...
// 2. Esegui la logica di logging
                .doOnNext(list -> {
                    if (list.size() > 0) { // Logga solo se ci sono transazioni
                        log.info("Found {} transactions SUSPENDED for batch {}",
                                list.size(),
                                oldBatchId);
                    }
                })
// ...

                // 3. Riconvertiamo la List in Flux per l'elaborazione (flatMap)
                .flatMapMany(Flux::fromIterable)
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchId(newBatchId);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then();

    }


    @Data
    public static class TotalAmount {
        private long total;
    }
}
