package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicy;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicyFactory;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.JwtUtils;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
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
    public Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId,
                                                                           String tokenPointOfSaleId,
                                                                           String initiativeId,
                                                                           String pointOfSaleId,
                                                                           String productGtin,
                                                                           String fiscalCode,
                                                                           String status,
                                                                           String trxCode,
                                                                           Pageable pageable) {
        String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
        String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);
        String sanitizeTokenPointOfSaleId = tokenPointOfSaleId == null ? null : Utilities.sanitizeString(tokenPointOfSaleId);
        String sanitizePointOfSaleId = pointOfSaleId == null ? null : Utilities.sanitizeString(pointOfSaleId);
        String sanitizeProductGtin = productGtin == null ? null : Utilities.sanitizeString(productGtin);
        String sanitizeFiscalCode = fiscalCode == null ? null : Utilities.sanitizeString(fiscalCode);
        String sanitizeStatus = status == null ? null : Utilities.sanitizeString(status);
        String sanitizeTrxCode = trxCode == null ? null : Utilities.sanitizeString(trxCode);
        log.info("[GET_POINT-OF-SALE_TRANSACTIONS] Point Of Sale {} requested to retrieve transactions", sanitizePointOfSaleId);

        if (tokenPointOfSaleId != null && (!sanitizeTokenPointOfSaleId
                .equals(sanitizePointOfSaleId))){

            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.FORBIDDEN,
                    ExceptionConstants.ExceptionCode.POINT_OF_SALE_NOT_ALLOWED,
                    String.format(
                            "Point of sale mismatch: expected [%s], but received [%s]", sanitizeTokenPointOfSaleId, sanitizePointOfSaleId
                    )
            ));
        }

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setFiscalCode(sanitizeFiscalCode);
        filters.setStatus(sanitizeStatus);
        filters.setTrxCode(sanitizeTrxCode);

        return pointOfSaleTransactionService.getPointOfSaleTransactions(sanitizeMerchantId, sanitizeInitiativeId, sanitizePointOfSaleId, sanitizeProductGtin, filters, pageable)
                .flatMap(page ->
                        Flux.fromIterable(page.getContent())
                                .flatMapSequential(trx -> mapper.toDTO(trx, initiativeId, fiscalCode))
                                .collectList()
                                .map(dtoList -> new PointOfSaleTransactionsListDTO(
                                        dtoList,
                                        page.getNumber(),
                                        page.getSize(),
                                        (int) page.getTotalElements(),
                                        page.getTotalPages()))
                );
    }

    @Override
    public Mono<DownloadInvoiceResponseDTO> downloadInvoiceFile(
            String merchantId, String tokenPointOfSaleId, String pointOfSaleId, String transactionId) {
        String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);
        String sanitizeTokenPointOfSaleId = tokenPointOfSaleId == null ? null : Utilities.sanitizeString(tokenPointOfSaleId);
        String sanitizePointOfSaleId = pointOfSaleId == null ? null : Utilities.sanitizeString(pointOfSaleId);
        String sanitizeTransactionId = transactionId == null ? null : Utilities.sanitizeString(transactionId);
        log.info("[DOWNLOAD_TRANSACTION] Requested to download invoice for transaction {}",
                sanitizeTransactionId);

        if (tokenPointOfSaleId != null && (!sanitizeTokenPointOfSaleId
                .equals(sanitizePointOfSaleId))){

            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.FORBIDDEN,
                    ExceptionConstants.ExceptionCode.POINT_OF_SALE_NOT_ALLOWED,
                    String.format(
                            "Point of sale mismatch: expected [%s], but received [%s]", sanitizeTokenPointOfSaleId, sanitizePointOfSaleId
                    )
            ));
        }

        return pointOfSaleTransactionService.downloadTransactionInvoice(sanitizeMerchantId, sanitizePointOfSaleId, sanitizeTransactionId);
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