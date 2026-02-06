package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantReportDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@Slf4j
public class MerchantTransactionControllerImpl implements MerchantTransactionController{
    private final MerchantTransactionService merchantTransactionService;
    public MerchantTransactionControllerImpl(MerchantTransactionService merchantTransactionService) {
        this.merchantTransactionService = merchantTransactionService;
    }


    @Override
    public Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId,
                                                                     String organizationRole,
                                                                     String initiativeId,
                                                                     String fiscalCode,
                                                                     String status,
                                                                     String rewardBatchId,
                                                                     String rewardBatchTrxStatus,
                                                                     String pointOfSaleId,
                                                                     String trxCode,
                                                                     Pageable pageable) {

      log.info("[GET_MERCHANT_TRANSACTIONS] Merchant {} requested to retrieve transactions", Utilities.sanitizeString(merchantId));
        return merchantTransactionService.getMerchantTransactions(merchantId, organizationRole, initiativeId, fiscalCode, status, rewardBatchId, rewardBatchTrxStatus, pointOfSaleId, trxCode, pageable);
    }

    @Override
    public Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole) {

        return merchantTransactionService.getProcessedTransactionStatuses(
                organizationRole);
    }


    @Override
    public Mono<MerchantReportDTO> getMerchantTransactionsReport(String merchantId,
                                                                 String organizationRole,
                                                                 String initiativeId,
                                                                 LocalDateTime startPeriod,
                                                                 LocalDateTime endPeriod,
                                                                 RewardBatchAssignee rewardBatchAssignee
                                                       ) {

        log.info("[GET_MERCHANT_TRANSACTIONS] Merchant {} requested to retrieve transactions", Utilities.sanitizeString(merchantId));
        return merchantTransactionService.getMerchantTransactionsReport(merchantId, organizationRole, initiativeId, startPeriod, endPeriod, rewardBatchAssignee);
    }

}
