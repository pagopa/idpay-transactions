package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

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
                                                                     Pageable pageable) {
        log.info("[GET_MERCHANT_TRANSACTIONS] Merchant {} requested to retrieve transactions", merchantId);
        return merchantTransactionService.getMerchantTransactions(
                merchantId,
                organizationRole,
                initiativeId,
                fiscalCode,
                status,
                rewardBatchId,
                rewardBatchTrxStatus,
                pointOfSaleId,
                pageable);
    }

    @Override
    public Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole) {

        log.info("[GET_MERCHANT_TRANSACTIONS_STATUSES] Requested statuses for role {}",
                organizationRole);

        return merchantTransactionService.getProcessedTransactionStatuses(
                organizationRole);
    }
}
