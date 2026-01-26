package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface MerchantTransactionService {
    Mono<MerchantTransactionsListDTO> getMerchantTransactions(String organizationRole,
                                                              TrxFiltersDTO filters,
                                                              Pageable pageable);

    Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole);

}
