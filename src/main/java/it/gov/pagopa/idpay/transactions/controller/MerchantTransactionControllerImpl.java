package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestBody;
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
    public Mono<MerchantTransactionsListDTO> getMerchantTransactions(
            String merchantId,
            String organizationRole,
            String initiativeId,
            TrxFiltersDTO filters,
            Pageable pageable) {

        filters.setMerchantId(merchantId);
        filters.setInitiativeId(initiativeId);
      log.info("[GET_MERCHANT_TRANSACTIONS] Merchant {} requested to retrieve transactions", Utilities.sanitizeString(merchantId));
        return merchantTransactionService.getMerchantTransactions(organizationRole, filters, pageable);
    }

    @Override
    public Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole) {

        return merchantTransactionService.getProcessedTransactionStatuses(
                organizationRole);
    }
}
