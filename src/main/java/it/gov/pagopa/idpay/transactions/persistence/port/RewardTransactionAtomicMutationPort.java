package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardTransactionAtomicMutationPort {

    Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String batchId,
            String transactionId,
            RewardBatchTrxStatus status,
            ReasonDTO reasons,
            String batchMonth,
            ChecksError checksError
    );
}
