package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

public interface RewardTransactionSpecificRepository {
    Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByFilter(TrxFiltersDTO filters, String userId, boolean includeToCheckWithConsultable, Pageable pageable);
    Mono<Long> getCount(TrxFiltersDTO filters, String pointOfSaleId, String productGtin, String userId, boolean includeToCheckWithConsultable);
    Mono<RewardTransaction> findOneByInitiativeId(String initiativeId);
    Mono<Void> removeInitiativeOnTransaction(String trxId, String initiativeId);
    Flux<RewardTransaction> findByInitiativesWithBatch(String initiativeId, int batchSize);
    Flux<RewardTransaction> findByFilterTrx(TrxFiltersDTO filters, String pointOfSaleId, String userId, String productGtin, boolean includeToCheckWithConsultable, Pageable pageable);

    Flux<RewardTransaction> findByFilter(String rewardBatchId, String initiativeId, List<RewardBatchTrxStatus> statusList);
    /**
     * Retrieves a transaction in status REWARDED, REFUNDED or INVOICED using the provided paramaters
     *
     * @param initiativeId
     * @param merchantId
     * @param transactionId
     * @return Mono containing a transaction, or empty if no document matches the criteria
     */
    Mono<RewardTransaction> findTransaction(String initiativeId, String merchantId, String transactionId);
    Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId);
    Mono<Long> sumSuspendedAccruedRewardCents(String initiativeId, String rewardBatchId, String merchantId);
    Mono<Void> rewardTransactionsByBatchIdAndInitiativeIdAndMerchantId(String batchId, String initiativeId, String merchantId);
    Mono<RewardTransaction> updateStatusAndReturnOld(String initiativeId, String merchantId, String batchId, String trxId, RewardBatchTrxStatus status, ReasonDTO reasons, String batchMonth, ChecksError checksError);
    Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(String initiativeId, String merchantId, int pageSize);
    Mono<RewardTransaction> findInvoicedTrxByIdWithoutBatch(String initiativeId, String merchantId, String trxId);
    Flux<FranchisePointOfSaleDTO> findDistinctFranchiseAndPosByRewardBatchId(String initiativeId, String merchantId, String rewardBatchId);
    Mono<RewardTransaction> findTransactionInBatch(String initiativeId, String merchantId, String rewardBatchId, String transactionId);

}
