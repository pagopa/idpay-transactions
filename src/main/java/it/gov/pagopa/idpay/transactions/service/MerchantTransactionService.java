package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface MerchantTransactionService {
    Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId,
                                                              String organizationRole,
                                                              String initiativeId,
                                                              String fiscalCode,
                                                              String status,
                                                              String rewardBatchId,
                                                              String rewardBatchTrxStatus,
                                                              String pointOfSaleId,
                                                              Pageable pageable);

    Mono<List<String>> getProcessedTransactionStatuses(
            String merchantId,
            String organizationRole,
            String initiativeId);

}
