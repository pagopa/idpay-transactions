package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchListPort {

    Flux<RewardBatch> findRewardBatches(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator,
            Pageable pageable
    );

    Mono<Long> countRewardBatches(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator
    );

    Flux<RewardBatch> findBatchesBeforeMonth(
            String merchantId,
            String initiativeId,
            PosType posType,
            String month
    );
}
