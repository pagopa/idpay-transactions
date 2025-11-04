package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE;

@Service
@Slf4j
public class PointOfSaleTransactionServiceImpl implements PointOfSaleTransactionService {

  private final UserRestClient userRestClient;
  private final RewardTransactionRepository rewardTransactionRepository;
  private final InvoiceStorageClient invoiceStorageClient;

  protected PointOfSaleTransactionServiceImpl(
      UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository, InvoiceStorageClient invoiceStorageClient) {
    this.userRestClient = userRestClient;
    this.rewardTransactionRepository = rewardTransactionRepository;
    this.invoiceStorageClient = invoiceStorageClient;
  }

  @Override
  public Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String productGtin, String fiscalCode, String status, Pageable pageable) {
    if (StringUtils.isNotBlank(fiscalCode)) {
      return userRestClient.retrieveFiscalCodeInfo(fiscalCode)
          .map(FiscalCodeInfoPDV::getToken)
          .flatMap(userId ->
              getTransactions(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status, pageable));
    } else {
      return getTransactions(merchantId, initiativeId, pointOfSaleId, null, productGtin, status, pageable);
    }
  }

  /**
   * Method to generate a download url of an invoice for a rewardTransaction in status REWARDED, REFUNDED or INVOICED,
   * the url will be provided with a Shared Access Signature token for the resource
   * @param merchantId
   * @param pointOfSaleId
   * @param transactionId
   * @return Mono containing the invoiceUrl, error if parameters do not match an existing transaction, or the invoice
   * reference is missing
   */
  @Override
  public Mono<DownloadInvoiceResponseDTO> downloadTransactionInvoice(
          String merchantId, String pointOfSaleId, String transactionId) {
      return rewardTransactionRepository.findTransaction(merchantId, pointOfSaleId, transactionId)
              .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
              .map(rewardTransaction -> {
                if (rewardTransaction.getInvoiceFile() == null ||
                        rewardTransaction.getInvoiceFile().getFilename() == null) {
                  throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE);
                }
                String filename = rewardTransaction.getInvoiceFile().getFilename();

                String blobPath = String.format("invoices/merchant/%s/pos/%s/transaction/%s/%s",
                    merchantId, pointOfSaleId, transactionId, filename);

                return DownloadInvoiceResponseDTO.builder()
                          .invoiceUrl(invoiceStorageClient.getFileSignedUrl(blobPath))
                          .build();
              });
  }

  private Mono<Page<RewardTransaction>> getTransactions(String merchantId, String initiativeId, String pointOfSaleId, String userId, String productGtin, String status, Pageable pageable) {
    return rewardTransactionRepository.findByFilterTrx(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status, pageable)
        .collectList()
        .zipWith(rewardTransactionRepository.getCount(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }
}
