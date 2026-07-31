package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.time.LocalDate;
import reactor.core.publisher.Mono;

public interface MerchantTransactionPostponementPort {

    Mono<RewardTransaction> postponeTransaction(
            String merchantId,
            String initiativeId,
            String sourceBatchId,
            String transactionId,
            LocalDate initiativeFruitionEndDate
    );
}
