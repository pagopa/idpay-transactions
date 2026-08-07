package it.gov.pagopa.idpay.transactions.service;

import com.google.common.hash.Hashing;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_NOT_FOUND;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoicedTransactionAssignmentPort;
import it.gov.pagopa.idpay.transactions.persistence.port.PaymentRewardBatchImpactPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSynchronizationPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSearchPort;
import it.gov.pagopa.idpay.transactions.utils.Utilities;

import java.time.LocalDate;
import java.time.YearMonth;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@Slf4j
public class RewardTransactionServiceImpl implements RewardTransactionService {

    private final RewardTransactionSearchPort rewardTransactionSearchPort;
    private final RewardTransactionSynchronizationPort rewardTransactionSynchronizationPort;
    private final InvoicedTransactionAssignmentPort invoicedTransactionAssignmentPort;
    private final PaymentRewardBatchImpactPort paymentRewardBatchImpactPort;
    private final MerchantRestClient merchantRestClient;
    private final int seed;


    public RewardTransactionServiceImpl(RewardTransactionSearchPort rewardTransactionSearchPort,
                                        RewardTransactionSynchronizationPort rewardTransactionSynchronizationPort,
                                        InvoicedTransactionAssignmentPort invoicedTransactionAssignmentPort,
                                        PaymentRewardBatchImpactPort paymentRewardBatchImpactPort,
                                        MerchantRestClient merchantRestClient,
                                        @Value(value="${app.sampling}") int seed) {
        this.rewardTransactionSearchPort = rewardTransactionSearchPort;
        this.rewardTransactionSynchronizationPort = rewardTransactionSynchronizationPort;
        this.invoicedTransactionAssignmentPort = invoicedTransactionAssignmentPort;
        this.paymentRewardBatchImpactPort = paymentRewardBatchImpactPort;
        this.merchantRestClient = merchantRestClient;
        this.seed = seed;
    }

    @Override
    public Mono<RewardTransaction> save(RewardTransaction rewardTransaction) {

        if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(rewardTransaction.getStatus())) {
            return enrichBatchData(rewardTransaction);
        }
        return rewardTransactionSynchronizationPort.upsert(rewardTransaction);
    }

    @Override
    public Mono<PaymentBatchEligibility> findEligibility(String merchantId, String transactionId) {
        return paymentRewardBatchImpactPort.findEligibility(merchantId, transactionId);
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        return rewardTransactionSearchPort.findByIdTrxIssuer(
                idTrxIssuer,
                userId,
                trxDateStart,
                trxDateEnd,
                amountCents,
                pageable
        );
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        return rewardTransactionSearchPort.findByRange(
                userId,
                trxDateStart,
                trxDateEnd,
                amountCents,
                pageable
        );
    }

    @Override
    public Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId){
        return rewardTransactionSearchPort.findByInitiativeIdAndUserId(initiativeId, userId);
    }

    @Override
    public Mono<Void> assignInvoicedTransactionsToBatches(
            Integer chunkSize,
            Integer repetitionsNumber,
            boolean processAll,
            String trxId) {

        if (trxId != null && !trxId.isEmpty()) {
            log.info("[BATCH_ASSIGNMENT] Processing transaction with ID={}", Utilities.sanitizeString(trxId));

            return invoicedTransactionAssignmentPort.findInvoicedTransactionWithoutBatch(trxId)
                    .switchIfEmpty(Mono.error(new ClientExceptionNoBody(
                            HttpStatus.NOT_FOUND,
                            String.format(TRANSACTION_NOT_FOUND, trxId))))
                    .flatMap(this::processTransaction)
                    .then();
        }

        if (processAll) {
            return processAllOperation(chunkSize);
        } else {
            return processSingleOperation(chunkSize, repetitionsNumber);
        }
    }

    private Mono<Void> processAllOperation(int chunkSize) {
      return invoicedTransactionAssignmentPort.findInvoicedTransactionsWithoutBatch(chunkSize)
          .collectList()
          .flatMap(list -> {
            if (list.isEmpty()) {
              log.info("[BATCH_ASSIGNMENT] Completed. No more transactions to process.");
              return Mono.empty();
            }

            log.info("[BATCH_ASSIGNMENT] Processing {} transactions...", list.size());

            return Flux.fromIterable(list)
                .concatMap(trx ->
                    processTransaction(trx)
                        .onErrorResume(e ->{
                          log.error("[BATCH_ERROR_TRANSACTION[{}] Error while processing transaction: {}",
                              trx.getId(), e.getMessage(), e);
                          return Mono.empty();
                        }))
                .then(Mono.defer(() -> processAllOperation(chunkSize)));
          });
    }

  private Mono<Void> processSingleOperation(int chunkSize, int repetitionsNumber) {

    return Flux.range(1, repetitionsNumber)
        .concatMap(i ->
            invoicedTransactionAssignmentPort.findInvoicedTransactionsWithoutBatch(chunkSize)
                .collectList()
                .flatMap(list -> {
                  if (list.isEmpty()) {
                    log.info("[BATCH_ASSIGNMENT] Completed at iteration {}. No transactions to process.", i);
                    return Mono.empty();
                  }
                  log.info("[BATCH_ASSIGNMENT] Iteration {}: Processing {} transactions.",
                      i, list.size());
                  return Flux.fromIterable(list)
                      .concatMap(trx ->
                          processTransaction(trx)
                              .onErrorResume(e-> {
                                log.error("[BATCH_ERROR_TRANSACTION[{}] Error while processing transaction: {}", trx.getId(), e.getMessage(), e);
                                return Mono.empty();
                              }))
                      .then();
                })
        )
        .then();
  }

  private Mono<RewardTransaction> processTransaction(RewardTransaction trx) {
      log.info("[BATCH_ASSIGNMENT][{}] Start processing transaction with status='{}', rewardBatchId='{}'",
          trx.getId(), trx.getStatus(), trx.getRewardBatchId());

      return setTrxMissingFields(trx)
          .flatMap(this::enrichBatchData)
          .doOnSuccess(savedTrx -> {
            log.info("[BATCH_ASSIGNMENT][{}] Transaction processed successfully.", savedTrx.getId());

            log.info("[BATCH_ASSIGNMENT][{}] Enriched fields: "
                    + "franchiseName='{}', pointOfSaleType='{}', businessName='{}', rewardBatchId='{}'",
                savedTrx.getId(),
                savedTrx.getFranchiseName(),
                savedTrx.getPointOfSaleType(),
                savedTrx.getBusinessName(),
                savedTrx.getRewardBatchId()
            );
          });
    }

    private Mono<RewardTransaction> setTrxMissingFields(RewardTransaction trx) {

      if (trx.getInvoiceUploadDate() == null) {
        trx.setInvoiceUploadDate(trx.getTrxChargeDate());
        log.info("[BATCH_ASSIGNMENT][{}] invoiceUploadDate was null, set to trxChargeDate={}",
            trx.getId(), trx.getTrxChargeDate());
      }

      return merchantRestClient.getPointOfSale(trx.getMerchantId(), trx.getPointOfSaleId())
          .map(pos -> {

            boolean enriched = false;

            if (trx.getFranchiseName() == null) {
              trx.setFranchiseName(pos.getFranchiseName());
              log.info("[BATCH_ASSIGNMENT][{}] franchiseName set to '{}'", trx.getId(), pos.getFranchiseName());
              enriched = true;
            }

            if (trx.getPointOfSaleType() == null) {
              trx.setPointOfSaleType(PosType.valueOf(pos.getType().name()));
              log.info("[BATCH_ASSIGNMENT][{}] pointOfSaleType set to '{}'", trx.getId(), pos.getType());
              enriched = true;
            }

            if (trx.getBusinessName() == null) {
              trx.setBusinessName(pos.getBusinessName());
              log.info("[BATCH_ASSIGNMENT][{}] businessName set to '{}'", trx.getId(), pos.getBusinessName());
              enriched = true;
            }

            if (!enriched) {
              log.info("[BATCH_ASSIGNMENT][{}] No enrichment needed, all fields already present", trx.getId());
            }

            return trx;
          });
    }

    private Mono<RewardTransaction> enrichBatchData(RewardTransaction trx) {

        LocalDate trxDate = trx.getInvoiceUploadDate() != null ?
                trx.getInvoiceUploadDate().toLocalDate() :  trx.getTrxChargeDate().toLocalDate();
        YearMonth trxMonth = YearMonth.from(trxDate);
        String batchMonth = trxMonth.toString();

        String initiativeId = trx.getInitiatives().getFirst();

        return invoicedTransactionAssignmentPort.assignInvoicedTransaction(
                trx,
                RewardBatchFactory.create(
                        initiativeId,
                        trx.getMerchantId(),
                        trx.getPointOfSaleType(),
                        batchMonth,
                        trx.getBusinessName()
                ),
                computeSamplingKey(trx.getId())
        );
    }

  /**
   * Computes a deterministic 32-bit sampling key from the given id string.
   * <p>
   * The same id combined with the same seed will always produce the same
   * sampling key. Different ids are expected to be uniformly distributed
   * over the 32-bit integer space, which makes this value suitable for
   * random-like ordering and sampling (e.g. sorting by this key and taking
   * the first N elements).
   *
   * @param id the identifier of the transaction (or any unique string); must not be {@code null}
   * @return a 32-bit signed integer representing the sampling key
   */
  public int computeSamplingKey(String id) {
    return Hashing
        .murmur3_32_fixed(this.seed)
        .hashUnencodedChars(id)
        .asInt();
  }
}
