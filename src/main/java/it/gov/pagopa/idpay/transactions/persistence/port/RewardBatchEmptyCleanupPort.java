package it.gov.pagopa.idpay.transactions.persistence.port;

import reactor.core.publisher.Mono;

public interface RewardBatchEmptyCleanupPort {

    Mono<Void> deleteEmptyBatches();
}
