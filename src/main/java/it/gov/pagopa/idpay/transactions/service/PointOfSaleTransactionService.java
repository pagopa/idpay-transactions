package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface PointOfSaleTransactionService {

  Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String fiscalCode, String status, Pageable pageable);

}
