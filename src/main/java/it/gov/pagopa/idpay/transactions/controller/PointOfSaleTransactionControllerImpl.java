package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PathVariable;
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
  public Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId, String tokenPointOfSaleId, String initiativeId, String pointOfSaleId, String productGtin, String fiscalCode, String status, Pageable pageable) {
    log.info("[GET_POINT-OF-SALE_TRANSACTIONS] Point Of Sale {} requested to retrieve transactions", Utilities.sanitizeString(pointOfSaleId));

    if (tokenPointOfSaleId != null && (!Utilities.sanitizeString(tokenPointOfSaleId)
        .equals(Utilities.sanitizeString(pointOfSaleId)))){

      return Mono.error(new ClientExceptionWithBody(
          HttpStatus.FORBIDDEN,
          ExceptionConstants.ExceptionCode.POINT_OF_SALE_NOT_ALLOWED,
          String.format(
              "Point of sale mismatch: expected [%s], but received [%s]", tokenPointOfSaleId, pointOfSaleId
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
          String merchantId, String tokenPointOfSaleId, String pointOfSaleId, String transactionId) {
    log.info("[DOWNLOAD_TRANSACTION] Requested to download invoice for transaction {}",
            Utilities.sanitizeString(transactionId));

    if (tokenPointOfSaleId != null && (!Utilities.sanitizeString(tokenPointOfSaleId)
        .equals(Utilities.sanitizeString(pointOfSaleId)))){

      return Mono.error(new ClientExceptionWithBody(
          HttpStatus.FORBIDDEN,
          ExceptionConstants.ExceptionCode.POINT_OF_SALE_NOT_ALLOWED,
          String.format(
              "Point of sale mismatch: expected [%s], but received [%s]", tokenPointOfSaleId, pointOfSaleId
          )
      ));
    }

    return pointOfSaleTransactionService.downloadTransactionInvoice(merchantId, pointOfSaleId, transactionId);
  }

  @Override
  public Mono<Void> updateInvoiceFile(String transactionId, String merchantId, String pointOfSaleId,
                                      FilePart file, String docNumber) {
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

    @Override
    public Mono<List<FranchisePointOfSaleDTO>> getFranchisePointOfSale(String rewardBatchId) {
      log.info("[POINT_OF_SALE_TRANSACTION_CONTROLLER] - Get point of sales by reward batch id {}", rewardBatchId);
      return pointOfSaleTransactionService.getDistinctFranchiseAndPosByRewardBatchId(rewardBatchId);
    }

}
