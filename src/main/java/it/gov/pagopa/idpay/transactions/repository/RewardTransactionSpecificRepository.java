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
     * @param merchantId
     * @param transactionId
     * @return Mono containing a transaction, or empty if no document matches the criteria
     */
    //aggiungere initiativeId
    Mono<RewardTransaction> findTransaction(String merchantId, String transactionId);

    Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId);
    //aggiungere merchantId
    Mono<Long> sumSuspendedAccruedRewardCents(String rewardBatchId);
    //aggiungere merchantId
    Mono<Void> rewardTransactionsByBatchId(String batchId);
    //aggiungere merchantId
    Mono<RewardTransaction> updateStatusAndReturnOld(String batchId, String trxId, RewardBatchTrxStatus status, ReasonDTO reasons, String batchMonth, ChecksError checksError);
    //aggiungere initiativeId
    Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(int pageSize);

    Mono<RewardTransaction> findInvoicedTrxByIdWithoutBatch(String trxId);

    //aggiungere initiativeId e merchantId
    Flux<FranchisePointOfSaleDTO> findDistinctFranchiseAndPosByRewardBatchId(String rewardBatchId);
    //aggiungere initiativeId
    Mono<RewardTransaction> findTransactionInBatch(String merchantId, String rewardBatchId, String transactionId);

}
