package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicy;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicyFactory;
import it.gov.pagopa.idpay.transactions.utils.JwtUtils;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@Slf4j
public class PointOfSaleTransactionControllerImpl implements PointOfSaleTransactionController {

    private final PointOfSaleTransactionService pointOfSaleTransactionService;
    private final PointOfSaleTransactionMapper mapper;

    public PointOfSaleTransactionControllerImpl(PointOfSaleTransactionService pointOfSaleTransactionService,
                                                PointOfSaleTransactionMapper mapper) {
        this.pointOfSaleTransactionService = pointOfSaleTransactionService;
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> updateInvoiceFile(String transactionId, String merchantId,
                                        FilePart file, String docNumber, String authorization) {
        String sanitizeTransactionId = transactionId == null ? null : Utilities.sanitizeString(transactionId);
        final String sanitizedMerchantId = Utilities.sanitizeString(merchantId);
        final String sanitizedTrxCode = transactionId == null ? null : Utilities.sanitizeString(transactionId);

        log.info(
                "[UPDATE_INVOICE_TRANSACTION] The merchant {} is requesting a invoice update for the transactionId {}",
                sanitizedMerchantId, sanitizedTrxCode
        );

        List<String> scopes = JwtUtils.extractScopesOrThrow(authorization);

        InvoiceLifecyclePolicy policy = InvoiceLifecyclePolicyFactory.fromScopes(scopes);

        return pointOfSaleTransactionService.updateInvoiceTransaction(sanitizeTransactionId, sanitizedMerchantId,
                file, docNumber, policy);
    }

    @Override
    public Mono<List<FranchisePointOfSaleDTO>> getFranchisePointOfSale(String rewardBatchId, String merchantId) {

        final String sanitizedRewardBatchId = Utilities.sanitizeString(rewardBatchId);
        String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);

        log.info("[POINT_OF_SALE_TRANSACTION_CONTROLLER] - Get point of sales by reward batch id {}", sanitizedRewardBatchId);

        return pointOfSaleTransactionService.getDistinctFranchiseAndPosByRewardBatchId(sanitizedRewardBatchId, sanitizeMerchantId);
    }

    @Override
    public Mono<Void> reversalTransactionInvoiced(String transactionId, String merchantId, String authorization, FilePart file, String docNumber) {

        final String sanitizedMerchantId = Utilities.sanitizeString(merchantId);
        final String sanitizedTrxCode = Utilities.sanitizeString(transactionId);
        String sanitizeTransactionId = transactionId == null ? null : Utilities.sanitizeString(transactionId);

        log.info(
                "[REVERSAL_TRANSACTION] The merchant {} is requesting a reversal for the transactionId {}",
                sanitizedMerchantId, sanitizedTrxCode
        );

        List<String> scopes = JwtUtils.extractScopesOrThrow(authorization);

        InvoiceLifecyclePolicy policy = InvoiceLifecyclePolicyFactory.fromScopes(scopes);

        return pointOfSaleTransactionService.reversalTransaction(sanitizeTransactionId, sanitizedMerchantId, file, docNumber, policy);
    }
}