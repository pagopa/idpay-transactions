package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface MerchantTransactionService {
    Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId,
                                                              OrganizationRole organizationRole,
                                                              String initiativeId,
                                                              String fiscalCode,
                                                              String status,
                                                              String rewardBatchId,
                                                              String rewardBatchTrxStatus,
                                                              Pageable pageable);
}
