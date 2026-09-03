package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchTransactionReadPort {

    Flux<RewardTransaction> findBatchTransactions(
            String rewardBatchId,
            String initiativeId,
            List<RewardBatchTrxStatus> statuses
    );

    Mono<RewardTransaction> findTransactionInBatch(
            String initiativeId,
            String merchantId,
            String rewardBatchId,
            String transactionId
    );

    Flux<FranchisePointOfSaleDTO> findDistinctFranchiseAndPosByRewardBatchId(
            String rewardBatchId,
            String merchantId
    );

    Flux<String> findBatchTransactionIds(String rewardBatchId, String initiativeId);
}
