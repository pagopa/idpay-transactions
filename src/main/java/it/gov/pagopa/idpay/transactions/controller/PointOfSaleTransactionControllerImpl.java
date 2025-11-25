package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
  public Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String productGtin, String fiscalCode, String status, Pageable pageable) {
    log.info("[GET_POINT-OF-SALE_TRANSACTIONS] Point Of Sale {} requested to retrieve transactions", Utilities.sanitizeString(pointOfSaleId));

    return pointOfSaleTransactionService.getPointOfSaleTransactions(merchantId, initiativeId, pointOfSaleId, productGtin, fiscalCode, status, pageable)
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
          String merchantId, String pointOfSaleId, String transactionId) {
    log.info("[DOWNLOAD_TRANSACTION] Requested to download invoice for transaction {}",
            Utilities.sanitizeString(transactionId));
    return pointOfSaleTransactionService.downloadTransactionInvoice(merchantId, pointOfSaleId, transactionId);
  }

  @Override
  public Mono<Void> updateInvoiceFile(String transactionId, String merchantId, String pointOfSaleId,
      MultipartFile file, String docNumber) {
    final String sanitizedMerchantId = Utilities.sanitizeString(merchantId);
    final String sanitizedTrxCode = Utilities.sanitizeString(transactionId);
    final String sanitizedPointOfSaleId = Utilities.sanitizeString(pointOfSaleId);

    log.info(
        "[UPDATE_INVOICE_TRANSACTION] The merchant {} is requesting a invoice update for the transactionId {} at POS {}",
        sanitizedMerchantId, sanitizedTrxCode, sanitizedPointOfSaleId
    );
    return pointOfSaleTransactionService.updateInvoiceTransaction(transactionId, merchantId,
        pointOfSaleId, file, docNumber);
  }
}
