package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface MerchantTransactionService {

    Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole);

}
