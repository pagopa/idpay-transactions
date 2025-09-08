package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.util.List;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public interface PointOfSaleTransactionService {

  Mono<Tuple2<List<RewardTransaction>, Long>> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String fiscalCode, String status, Pageable pageable);

}
