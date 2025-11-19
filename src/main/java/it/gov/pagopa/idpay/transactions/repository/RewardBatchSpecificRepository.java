package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchSpecificRepository {
  Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, Pageable pageable);
  Mono<Long> getCount(String merchantId);
}
