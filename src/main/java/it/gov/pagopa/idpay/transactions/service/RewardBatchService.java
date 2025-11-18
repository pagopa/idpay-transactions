package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.BatchType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.YearMonth;
import reactor.core.publisher.Mono;

public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, YearMonth month, BatchType batchType);
}
