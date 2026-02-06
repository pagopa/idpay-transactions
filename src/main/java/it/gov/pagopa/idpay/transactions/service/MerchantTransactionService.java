package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.MerchantReportDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
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
                                                              String trxCode,
                                                              Pageable pageable);

    Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole);

    Mono<MerchantReportDTO> generateMerchantTransactionsReport(String merchantId,
                                                          String organizationRole,
                                                          String initiativeId,
                                                          LocalDateTime startPeriod,
                                                          LocalDateTime endPeriod,
                                                          RewardBatchAssignee rewardBatchAssignee);

}
