package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import lombok.Data;
import org.springframework.dao.DuplicateKeyException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.List;
import java.util.Locale;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class RewardBatchServiceImpl implements RewardBatchService {

  private final RewardBatchRepository rewardBatchRepository;
  private final RewardTransactionRepository rewardTransactionRepository;
  private final ReactiveMongoTemplate reactiveMongoTemplate;

  public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository, RewardTransactionRepository rewardTransactionRepository) {
  public RewardBatchServiceImpl(RewardBatchRepository rewardBatchRepository, ReactiveMongoTemplate reactiveMongoTemplate) {
    this.rewardBatchRepository = rewardBatchRepository;
    this.rewardTransactionRepository = rewardTransactionRepository;
    this.reactiveMongoTemplate = reactiveMongoTemplate;
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
  public Mono<Page<RewardBatch>> getMerchantRewardBatches(String merchantId, Pageable pageable) {
    return rewardBatchRepository.findRewardBatchByMerchantId(merchantId, pageable)
        .collectList()
        .zipWith(rewardBatchRepository.getCount(merchantId))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }

  @Override
  public Mono<Page<RewardBatch>> getAllRewardBatches(Pageable pageable) {
    return rewardBatchRepository.findRewardBatch(pageable)
        .collectList()
        .zipWith(rewardBatchRepository.getCount())
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
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
        .totalAmountCents(0L)
        .approvedAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .reportPath(null)
        .assigneeLevel(RewardBatchAssignee.L1)
        .numberOfTransactionsSuspended(0L)
        .numberOfTransactionsRejected(0L)
        .build();

    return rewardBatchRepository.save(batch);
  }

  @Override
  public Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents) {
    return rewardBatchRepository.incrementTotals(batchId, accruedAmountCents);
  }

    @Override
    public Mono<RewardBatch> suspendTransactions(String rewardBatchId, TransactionsRequest request) {
        return rewardBatchRepository.findById(rewardBatchId)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Batch not found: " + rewardBatchId)))
                .flatMap(batch -> {

                    if (RewardBatchStatus.APPROVED.equals(batch.getStatus())) {
                        log.info("Batch {} APPROVED, skipping suspension", rewardBatchId);
                        return Mono.error(new IllegalStateException("Cannot suspend transactions on an APPROVED batch"));
                    }

                    return updateTransactionsStatus(
                            rewardBatchId,
                            request.getTransactionIds(),
                            RewardBatchTrxStatus.SUSPENDED,
                            request.getReason()
                    ).flatMap(modifiedCount -> {
                        //da modificare, se 0 c'è stato un errore
                        if (modifiedCount == 0) {
                            return Mono.just(batch);
                        }

                        MatchOperation match = Aggregation.match(
                                Criteria.where("rewardBatchId").is(rewardBatchId)
                                        .and("id").in(request.getTransactionIds())
                                        .and("rewardBatchTrxStatus").is(RewardBatchTrxStatus.SUSPENDED)
                        );

                        Aggregation agg = Aggregation.newAggregation(
                                match,
                                //dubbio se sia accruedRewardCents
                                Aggregation.group().sum("amountCents").as("total")
                        );

                        return Mono.from(reactiveMongoTemplate.aggregate(agg, RewardTransaction.class, TotalAmount.class)
                                        .next())
                                .defaultIfEmpty(new TotalAmount())
                                .flatMap(totalAmount -> {
                                    long suspendedTotal = totalAmount.getTotal();

                                    Update update = new Update()
                                            .inc("numberOfTransactionsElaborated", modifiedCount)
                                            .inc("approvedAmountCents", -suspendedTotal)
                                            .inc("numberOfTransactionsSuspended", modifiedCount)
                                            .currentDate("updateDate");

                                    Query query = Query.query(Criteria.where("_id").is(rewardBatchId));
                                    return reactiveMongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), RewardBatch.class);
                                });
                    });
                });
    }

    @Override
    public Mono<Long> updateTransactionsStatus(String rewardBatchId, List<String> transactionIds, RewardBatchTrxStatus newStatus, String reason) {
        Query query = new Query(
                Criteria.where("rewardBatchId").is(rewardBatchId)
                        .and("id").in(transactionIds));

        Update update = new Update()
                .set("rewardBatchTrxStatus", newStatus)
                .set("rewardBatchRejectionReason", reason);

        return reactiveMongoTemplate.updateMulti(query, update, RewardTransaction.class)
                .map(UpdateResult::getModifiedCount);
    }

    private String buildBatchName(YearMonth month) {
    String monthName = month.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN);
    String year = String.valueOf(month.getYear());

    return String.format("%s %s", monthName, year);
  }


  @Override
  public  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId) {
    return updateAndSaveRewardBatch(rewardBatchId)
            .flatMap(newBatch -> {
              String newBatchId = newBatch.getId();
                Mono<Void> transactionsUpdate = updateAndSaveRewardTransactions(rewardBatchId, initiativeId, newBatchId);
                return transactionsUpdate.thenReturn(newBatch);

            });
            //.thenReturn(ResponseEntity.noContent().build())
            //.defaultIfEmpty(ResponseEntity.notFound().build());
  }

  Mono<RewardBatch> updateAndSaveRewardBatch(String rewardBatchId){
    Mono<RewardBatch> monoRewardBatch = rewardBatchRepository.findRewardBatchById(rewardBatchId);
    return monoRewardBatch.map(rewardBatch -> {
              rewardBatch.setStatus(RewardBatchStatus.APPROVED);
              rewardBatch.setUpdateDate(Instant.now());
              return rewardBatch;
            })
            .flatMap(rewardBatchRepository::save)
            .flatMap(this::createRewardBatchAndSave);
  }
  Mono<RewardBatch> createRewardBatchAndSave(RewardBatch savedBatch) {
    RewardBatch newBatch = RewardBatch.builder()
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
            .totalAmountCents(savedBatch.getTotalAmountCents()) //METTI 0
            .numberOfTransactions(savedBatch.getNumberOfTransactions()) //DA MODIFICARE DOPO
            .numberOfTransactionsElaborated(savedBatch.getNumberOfTransactionsElaborated())
            .updateDate(savedBatch.getUpdateDate())
            .build();
    return rewardBatchRepository.save(newBatch);
  }

    public String addOneMonth(String yearMonthString) {

        // 1. Definisci il Formatter per il formato "AAAA-MM"
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM");

        // 2. Parsa la stringa in un oggetto YearMonth
        YearMonth yearMonth = YearMonth.parse(yearMonthString, inputFormatter);

        // 3. Aggiungi 1 Mese
        YearMonth nextYearMonth = yearMonth.plusMonths(1);

        // 4. Formatta il risultato in una nuova stringa "AAAA-MM"
        // Il formatter è lo stesso, ma puoi riutilizzarlo o usare il metodo di default
        return nextYearMonth.format(inputFormatter);
    }


    public String addOneMonthToItalian(String italianMonthString) {

        // 1. Definisci il Formatter con il pattern corretto e il Locale IT
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM yyyy", Locale.ITALIAN);

        // 2. Parsa la stringa in un oggetto YearMonth
        YearMonth yearMonth = YearMonth.parse(italianMonthString, formatter);

        // 3. Aggiungi 1 Mese
        YearMonth nextYearMonth = yearMonth.plusMonths(1);

        // 4. Formatta il risultato nella nuova stringa
        return nextYearMonth.format(formatter);
    }

    public  Mono<Void> updateAndSaveRewardTransactions(String oldBatchId, String initiativeId, String newBatchId) {

      Mono<Void> approveCheckTransactions = rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, "TO_CHECK")
              .flatMap(rewardTransaction -> {
                rewardTransaction.setStatus("APPROVED");
                return rewardTransactionRepository.save(rewardTransaction);
              })
              .then(); // Converte Flux<RewardTransaction> in Mono<Void>

      Mono<Void> approveConsultableTransactions = rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, "CONSULTABLE")
              .flatMap(rewardTransaction -> {
                rewardTransaction.setStatus("APPROVED");
                return rewardTransactionRepository.save(rewardTransaction);
              })
              .then(); // Converte Flux<RewardTransaction> in Mono<Void>

      // 2. Definiamo l'operazione condizionale per SUSPENDED (Deve aspettare l'approvazione del nuovo batch ID)
      Mono<Void> handleSuspendedTransactions = rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, "SUSPENDED")
              .collectList() // Raccoglie tutte le transazioni SUSPENDED in un Mono<List<RewardTransaction>>
              .flatMap(suspendedList -> {
                if (suspendedList.isEmpty()) {
                  // Caso A: Nessuna transazione sospesa trovata. Dobbiamo eliminare il nuovo batch appena creato.
                  System.out.println("Nessuna transazione SUSPENDED trovata. Eliminazione del nuovo batch: " + newBatchId);
                  return rewardBatchRepository.deleteById(newBatchId)
                          .then(Mono.empty()); // Continua il flusso ma emette solo il segnale di completamento
                } else {
                  // Caso B: Trovate transazioni sospese. Aggiorna il loro Batch ID e salva.
                  System.out.println("Trovate " + suspendedList.size() + " transazioni SUSPENDED. Aggiorno ID batch.");

                  // Crea un Flux dai risultati e aggiorna il Batch ID
                  return Flux.fromIterable(suspendedList)
                          .flatMap(rewardTransaction -> {
                            rewardTransaction.setRewardBatchId(newBatchId); // Assegna il nuovo ID
                            return rewardTransactionRepository.save(rewardTransaction);
                          })
                          .then(); // Converte il Flux di salvataggio in Mono<Void>
                }
              });

      // 3. Combina le operazioni sequenziali e parallele
      return Mono.when(approveCheckTransactions, approveConsultableTransactions) // Esegue in parallelo le due approvazioni
              .then(handleSuspendedTransactions); // Solo dopo, esegue la logica condizionale su SUSPENDED
    }


    @Override
    public Mono<RewardBatch> provaGet(String initiativeId, String rewardBatchId) {
        return getRewardBatch(rewardBatchId);
    }

    Mono<RewardBatch> getRewardBatch(String rewardBatchId){
        Mono<RewardBatch> monoRewardBatch = rewardBatchRepository.findRewardBatchById(rewardBatchId);
        return monoRewardBatch.map(rewardBatch -> {
                    rewardBatch.setStatus(RewardBatchStatus.APPROVED);
                    rewardBatch.setUpdateDate(Instant.now());
                    return rewardBatch;
                });
    }


    @Override
    public Mono<RewardBatch> provaSave(String initiativeId, String rewardBatchId) {
        return updateAndSaveRewardBatchProvaSave(rewardBatchId);
    }

    Mono<RewardBatch> updateAndSaveRewardBatchProvaSave(String rewardBatchId){
        Mono<RewardBatch> monoRewardBatch = rewardBatchRepository.findRewardBatchById(rewardBatchId);
        return monoRewardBatch.map(rewardBatch -> {
                    rewardBatch.setStatus(RewardBatchStatus.APPROVED);
                    rewardBatch.setUpdateDate(Instant.now());
                    return rewardBatch;
                })
                .flatMap(rewardBatchRepository::save);
    }

    @Override
    public Mono<RewardBatch> provaSaveAndCreateNewBatch(String initiativeId, String rewardBatchId) {
        return updateAndSaveRewardBatch(rewardBatchId);
    }



    @Data
    static class TotalAmount {
        private long total;
    }
}
