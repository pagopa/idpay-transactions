package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface MerchantTransactionService {
    Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId, String initiativeId, String fiscalCode, String status, Pageable pageable);
}
