package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.BatchType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.YearMonth;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, YearMonth month, BatchType batchType);

  Mono<Page<RewardBatch>> getRewardBatch(String merchantId, Pageable pageable);
}
