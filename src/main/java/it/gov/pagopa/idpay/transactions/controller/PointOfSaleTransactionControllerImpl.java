package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
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
  public Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId, String tokenPointOfSaleId, String initiativeId, String pointOfSaleId, String productGtin, String fiscalCode, String status, Pageable pageable) {
    log.info("[GET_POINT-OF-SALE_TRANSACTIONS] Point Of Sale {} requested to retrieve transactions", Utilities.sanitizeString(pointOfSaleId));

    if (!Utilities.sanitizeString(pointOfSaleId)
        .equals(Utilities.sanitizeString(tokenPointOfSaleId))) {

      return Mono.error(new ClientExceptionWithBody(
          HttpStatus.FORBIDDEN,
          ExceptionConstants.ExceptionCode.POINT_OF_SALE_NOT_ALLOWED,
          String.format(
              "The point-of-sale with id [%s] is not authorized for the current token [%s]",
              pointOfSaleId,
              tokenPointOfSaleId
          )
      ));
    }

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
}
