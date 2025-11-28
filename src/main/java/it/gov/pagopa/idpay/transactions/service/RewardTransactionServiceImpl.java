package it.gov.pagopa.idpay.transactions.service;

import com.google.common.hash.Hashing;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.time.LocalDate;
import java.time.YearMonth;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@Slf4j
public class RewardTransactionServiceImpl implements RewardTransactionService {

    private final RewardTransactionRepository rewardTrxRepository;
    private final RewardBatchService rewardBatchService;
    private final MerchantRestClient merchantRestClient;
    private final int seed;


    public RewardTransactionServiceImpl(RewardTransactionRepository rewardTrxRepository,
        RewardBatchService rewardBatchService,
        MerchantRestClient merchantRestClient,
        @Value(value="${app.sampling}") int seed) {
        this.rewardTrxRepository = rewardTrxRepository;
        this.rewardBatchService = rewardBatchService;
        this.merchantRestClient = merchantRestClient;
        this.seed = seed;
    }

    @Override
    public Mono<RewardTransaction> save(RewardTransaction rewardTransaction) {

        if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(rewardTransaction.getStatus())) {
            return enrichBatchData(rewardTransaction)
                .flatMap(rewardTrxRepository::save);
        }
        return rewardTrxRepository.save(rewardTransaction);
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        return rewardTrxRepository.findByIdTrxIssuer(idTrxIssuer, userId, trxDateStart, trxDateEnd, amountCents, pageable);
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        return rewardTrxRepository.findByRange(userId, trxDateStart, trxDateEnd, amountCents, pageable);
    }

    @Override
    public Mono<RewardTransaction> findByTrxIdAndUserId(String trxId, String userId){
        return rewardTrxRepository.findByTrxIdAndUserId(trxId, userId);
    }

    @Override
    public Mono<Void> assignInvoicedTransactionsToBatches(Integer chunkSize) {
        log.info("[BATCH_ASSIGNMENT] Using chunkSize={}", chunkSize);

        return processOperation(chunkSize);
    }

    private Mono<Void> processOperation(int chunkSize) {
        return rewardTrxRepository.findInvoicedTransactionsWithoutBatch(chunkSize)
            .collectList()
            .flatMap(list -> {
                if (list.isEmpty()) {
                    log.info("[BATCH_ASSIGNMENT] Completed. No more transactions to process.");
                    return Mono.empty();
                }

                log.info("[BATCH_ASSIGNMENT] Processing {} transactions...", list.size());

                return Flux.fromIterable(list)
                    .concatMap(this::processTransaction)
                    .then(Mono.defer(() -> processOperation(chunkSize)));
            });
    }

    private Mono<RewardTransaction> processTransaction(RewardTransaction trx) {

        return setTrxMissingFields(trx)
            .flatMap(this::enrichBatchData)
            .flatMap(rewardTrxRepository::save);
    }

    private Mono<RewardTransaction> setTrxMissingFields(RewardTransaction trx) {

        if (trx.getInvoiceUploadDate() == null) {
            trx.setInvoiceUploadDate(trx.getTrxChargeDate());
        }

        return merchantRestClient.getPointOfSale(trx.getMerchantId(), trx.getPointOfSaleId())
            .map(pos -> {

                if (trx.getFranchiseName() == null) {
                    trx.setFranchiseName(pos.getFranchiseName());
                }

                if (trx.getPointOfSaleType() == null) {
                    trx.setPointOfSaleType(PosType.valueOf(pos.getType().name()));
                }

                if (trx.getBusinessName() == null) {
                    trx.setBusinessName(pos.getBusinessName());
                }

                return trx;
            });
    }

    private Mono<RewardTransaction> enrichBatchData(RewardTransaction trx) {

        LocalDate trxDate = trx.getInvoiceUploadDate().toLocalDate();
        YearMonth trxMonth = YearMonth.from(trxDate);
        String batchMonth = trxMonth.toString();

        String initiativeId = trx.getInitiatives().get(0);

        long accruedRewardCents = trx.getRewards()
            .get(initiativeId)
            .getAccruedRewardCents();

        return rewardBatchService.findOrCreateBatch(
                    trx.getMerchantId(),
                    trx.getPointOfSaleType(),
                    batchMonth,
                    trx.getBusinessName()
            )
            .flatMap(rewardBatch ->
                rewardBatchService.incrementTotals(rewardBatch.getId(), accruedRewardCents)
                    .map(batch -> {
                        trx.setRewardBatchId(batch.getId());
                        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
                        trx.setRewardBatchInclusionDate(LocalDateTime.now());
                        trx.setRewardBatchRejectionReason(null);
                        trx.setSamplingKey(computeSamplingKey(trx.getId()));
                        return trx;
                    })
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
