package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

import java.io.IOException;

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
                String status = rewardTransaction.getStatus();
                InvoiceData documentData = null;
                String typeFolder;

                if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status) || SyncTrxStatus.REWARDED.name().equalsIgnoreCase(status)) {
                    documentData = rewardTransaction.getInvoiceData();
                    typeFolder = "invoice";
                }else {
                  throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE);
                }

                if (documentData == null || documentData.getFilename() == null) {
                  throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE);
                }

                String filename = documentData.getFilename();

                String blobPath = String.format("invoices/merchant/%s/pos/%s/transaction/%s/%s/%s",
                    merchantId, pointOfSaleId, transactionId, typeFolder, filename);

                return DownloadInvoiceResponseDTO.builder()
                          .invoiceUrl(invoiceStorageClient.getFileSignedUrl(blobPath))
                          .build();
              });
  }

  @Override
  public Mono<Void> updateInvoiceTransaction(String transactionId, String merchantId, String pointOfSaleId, MultipartFile file, String docNumber) {
    try {
      Utilities.checkFileExtensionOrThrow(file);

      return rewardTransactionRepository.findTransaction(merchantId, pointOfSaleId, transactionId)
          .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
          .flatMap(rewardTransaction -> {
            String status = rewardTransaction.getStatus();
            InvoiceData oldDocumentData = null;

            if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status)) {
              oldDocumentData = rewardTransaction.getInvoiceData();
            } else {
              throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE);
            }
            if (!rewardTransaction.getMerchantId().equals(merchantId)) {
              throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                  "The merchant with id [%s] associated to the transaction is not equal to the merchant with id [%s]".formatted(
                      rewardTransaction.getMerchantId(), merchantId));
            }
            if (!rewardTransaction.getPointOfSaleId().equals(pointOfSaleId)) {
              throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                  "The pointOfSaleId with id [%s] associated to the transaction is not equal to the pointOfSaleId with id [%s]".formatted(
                      rewardTransaction.getPointOfSaleId(), pointOfSaleId));
            }

            String oldFilename = oldDocumentData.getFilename();

            String blobPath = String.format(
                "invoices/merchant/%s/pos/%s/transaction/%s/invoice/%s",
                merchantId, pointOfSaleId, transactionId, file.getOriginalFilename());
            String oldBlobPath = String.format(
                "invoices/merchant/%s/pos/%s/transaction/%s/invoice/%s",
                merchantId, pointOfSaleId, transactionId, oldFilename);

            return Mono.fromCallable(() -> {
              // Delete old file from storage
              invoiceStorageClient.deleteFile(oldBlobPath);

              // Upload new file on storage
              invoiceStorageClient.upload(file.getInputStream(), blobPath, file.getContentType());
              return null;
            }).onErrorMap(IOException.class, e -> {
              log.error("Error uploading file to storage for transaction [{}]", transactionId, e);
              throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR, ExceptionConstants.ExceptionCode.GENERIC_ERROR, "Error uploading invoice file", e);
            }).then(Mono.fromRunnable(() -> {
              // Update Transaction document
              rewardTransaction.setInvoiceData(InvoiceData.builder()
                  .filename(file.getOriginalFilename())
                  .docNumber(docNumber)
                  .build());

            })).then(rewardTransactionRepository.save(rewardTransaction).then());
          });
    } catch (Exception e) {
      throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR, ExceptionConstants.ExceptionCode.GENERIC_ERROR, "Error uploading invoice file", e);
    }
  }

    private Mono<Page<RewardTransaction>> getTransactions(String merchantId, String initiativeId, String pointOfSaleId, String userId, String productGtin, String status, Pageable pageable) {
    return rewardTransactionRepository.findByFilterTrx(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status, pageable)
        .collectList()
        .zipWith(rewardTransactionRepository.getCount(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }
}
