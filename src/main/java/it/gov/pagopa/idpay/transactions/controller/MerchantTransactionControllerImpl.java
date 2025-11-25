package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.Arrays;
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
                                                                     OrganizationRole organizationRole,
                                                                     String initiativeId,
                                                                     String fiscalCode,
                                                                     String status,
                                                                     String rewardBatchId,
                                                                     String rewardBatchTrxStatus,
                                                                     Pageable pageable) {
        if (OrganizationRole.MERCHANT.equals(organizationRole)
                && "TO_CHECK".equalsIgnoreCase(rewardBatchTrxStatus)) {
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Status TO_CHECK not allowed for merchants"
            );
        }
        log.info("[GET_MERCHANT_TRANSACTIONS] Merchant {} requested to retrieve transactions", merchantId);
        return merchantTransactionService.getMerchantTransactions(
                merchantId,
                organizationRole,
                initiativeId,
                fiscalCode,
                status,
                rewardBatchId,
                rewardBatchTrxStatus,
                pageable);
    }

    @Override
    public Mono<List<String>> getProcessedTransactionStatuses(
            String merchantId,
            OrganizationRole organizationRole,
            String initiativeId) {

        List<String> allStatuses = Arrays.stream(RewardBatchTrxStatus.values())
                .map(Enum::name)
                .toList();

        List<String> result;
        if (OrganizationRole.MERCHANT.equals(organizationRole)) {
            result = allStatuses.stream()
                    .filter(s -> !"TO_CHECK".equalsIgnoreCase(s))
                    .toList();
        } else {
            result = allStatuses;
        }

        log.info("[GET_MERCHANT_TRANSACTIONS_STATUSES] Merchant {} with role {} requested statuses for initiative {}",
                merchantId, organizationRole, initiativeId);

        return Mono.just(result);
    }
}
