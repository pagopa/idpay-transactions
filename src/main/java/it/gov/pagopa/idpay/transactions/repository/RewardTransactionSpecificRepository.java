package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

public interface RewardTransactionSpecificRepository {
    Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByFilter(TrxFiltersDTO filters, String userId, Pageable pageable);
    Mono<Long> getCount(TrxFiltersDTO filters, String pointOfSaleId, String productGtin, String userId);
    Mono<RewardTransaction> findOneByInitiativeId(String initiativeId);
    Mono<Void> removeInitiativeOnTransaction(String trxId, String initiativeId);
    Flux<RewardTransaction> findByInitiativesWithBatch(String initiativeId, int batchSize);
    Flux<RewardTransaction> findByFilterTrx(TrxFiltersDTO filters, String pointOfSaleId, String userId, String productGtin, Pageable pageable);

    Flux<RewardTransaction> findByFilter(String rewardBatchId, String initiativeId, List<RewardBatchTrxStatus> statusList);
    /**
     * Retrieves a transaction in status REWARDED, REFUNDED or INVOICED using the provided paramaters
     * @param merchantId
     * @param pointOfSaleId
     * @param transactionId
     * @return Mono containing a transaction, or empty if no document matches the criteria
     */
    Mono<RewardTransaction> findTransaction(String merchantId, String pointOfSaleId, String transactionId);

    Mono<RewardTransaction> findByTrxIdAndUserId(String trxId, String userId);

    Mono<Long> sumSuspendedAccruedRewardCents(String rewardBatchId, List<String> transactionIds, String initiativeId);

    Mono<Void> rewardTransactionsByBatchId(String batchId);

    Mono<RewardTransaction> updateStatusAndReturnOld(String batchId, String trxId, RewardBatchTrxStatus status, String reason);
}
