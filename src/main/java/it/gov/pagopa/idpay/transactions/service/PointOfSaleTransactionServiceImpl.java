package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import java.time.LocalDateTime;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.YearMonth;

import static it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus.CREATED;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_ALREADY_SENT;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_REWARD_BATCH_ALREADY_SENT;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_NOT_FOUND;

@Service
@Slf4j
public class PointOfSaleTransactionServiceImpl implements PointOfSaleTransactionService {

  private final UserRestClient userRestClient;
  private final RewardTransactionRepository rewardTransactionRepository;
  private final InvoiceStorageClient invoiceStorageClient;
  private final RewardBatchService rewardBatchService;
  private final RewardBatchRepository rewardBatchRepository;

  protected PointOfSaleTransactionServiceImpl(
          UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository, InvoiceStorageClient invoiceStorageClient, RewardBatchService rewardBatchService,
      RewardBatchRepository rewardBatchRepository) {
    this.userRestClient = userRestClient;
    this.rewardTransactionRepository = rewardTransactionRepository;
    this.invoiceStorageClient = invoiceStorageClient;
    this.rewardBatchService = rewardBatchService;
    this.rewardBatchRepository = rewardBatchRepository;
  }

    @Override
    public Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId,
                                                                    String initiativeId,
                                                                    String pointOfSaleId,
                                                                    String productGtin,
                                                                    String fiscalCode,
                                                                    String status,
                                                                    Pageable pageable) {

        TrxFiltersDTO filters = new TrxFiltersDTO(
                merchantId,
                initiativeId,
                null,
                status,
                null,
                null,
                null
        );

        if (StringUtils.isNotBlank(fiscalCode)) {
            return userRestClient.retrieveFiscalCodeInfo(fiscalCode)
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId ->
                            getTransactions(filters, pointOfSaleId, userId, productGtin, pageable));
        } else {
            return getTransactions(filters, pointOfSaleId, null, productGtin, pageable);
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
                } else if (SyncTrxStatus.REFUNDED.name().equalsIgnoreCase(status)) {
                  documentData = rewardTransaction.getCreditNoteData();
                  typeFolder = "creditNote";
                } else {
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

    private Mono<Page<RewardTransaction>> getTransactions(TrxFiltersDTO filters,
                                                          String pointOfSaleId,
                                                          String userId,
                                                          String productGtin,
                                                          Pageable pageable) {

        boolean includeToCheckWithConsultable = false;

        return rewardTransactionRepository
                .findByFilterTrx(filters, pointOfSaleId, userId, productGtin, includeToCheckWithConsultable, pageable)
                .collectList()
                .zipWith(rewardTransactionRepository.getCount(
                        filters,
                        pointOfSaleId,
                        productGtin,
                        userId,
                        includeToCheckWithConsultable
                ))
                .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
    }

  public Mono<Void> updateInvoiceTransaction(String transactionId, String merchantId,
      String pointOfSaleId, FilePart file, String docNumber) {
    try {
      Utilities.checkFileExtensionOrThrow(file);

      return rewardTransactionRepository.findTransaction(merchantId, pointOfSaleId, transactionId)
          .switchIfEmpty(Mono.error(
              new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
          .flatMap(rewardTransaction -> {
            String status = rewardTransaction.getStatus();
            String rewardbatchId = rewardTransaction.getRewardBatchId();
            Mono<Void> batchStatusCheck = Mono.empty();
            if (rewardbatchId != null) {
              batchStatusCheck = rewardBatchRepository.findRewardBatchById(rewardbatchId)
                  .switchIfEmpty(Mono.error(
                      new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND)))
                  .flatMap(rewardBatch -> {
                    if (!CREATED.equals(rewardBatch.getStatus())) {
                      return Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
                          REWARD_BATCH_ALREADY_SENT,
                          ERROR_MESSAGE_REWARD_BATCH_ALREADY_SENT));
                    }
                    return Mono.empty();
                  });
            }

            return batchStatusCheck.then(Mono.defer(() -> {
              InvoiceData oldDocumentData = null;

              if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status)) {
                oldDocumentData = rewardTransaction.getInvoiceData();
              } else {
                throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST,
                    TRANSACTION_MISSING_INVOICE);
              }
              if (!rewardTransaction.getMerchantId().equals(merchantId)) {
                throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
                    ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                    "The merchant with id [%s] associated to the transaction is not equal to the merchant with id [%s]".formatted(
                        rewardTransaction.getMerchantId(), merchantId));
              }
              if (!rewardTransaction.getPointOfSaleId().equals(pointOfSaleId)) {
                throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
                    ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                    "The pointOfSaleId with id [%s] associated to the transaction is not equal to the pointOfSaleId with id [%s]".formatted(
                        rewardTransaction.getPointOfSaleId(), pointOfSaleId));
              }

              String oldFilename = oldDocumentData.getFilename();

              String blobPath = String.format(
                  "invoices/merchant/%s/pos/%s/transaction/%s/invoice/%s",
                  merchantId, pointOfSaleId, transactionId, file.filename());
              String oldBlobPath = String.format(
                  "invoices/merchant/%s/pos/%s/transaction/%s/invoice/%s",
                  merchantId, pointOfSaleId, transactionId, oldFilename);
              Path tempPath = Paths.get(System.getProperty("java.io.tmpdir"), file.filename());

              return file.transferTo(tempPath)
                  .then(Mono.fromCallable(() -> {
                    invoiceStorageClient.deleteFile(oldBlobPath);

                    try (InputStream is = Files.newInputStream(tempPath)) {
                      String contentType = file.headers().getContentType() != null
                          ? file.headers().getContentType().toString()
                          : null;
                      invoiceStorageClient.upload(is, blobPath, contentType);
                    }
                    return Boolean.TRUE;
                  }))
                  .onErrorMap(IOException.class, e -> {
                    log.error("Error uploading file to storage for transaction [{}]",
                        Utilities.sanitizeString(transactionId), e);
                    throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
                        ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                        "Error uploading invoice file", e);
                  })
                  .then(Mono.defer(() -> {
                    rewardTransaction.setInvoiceData(InvoiceData.builder()
                        .filename(file.filename())
                        .docNumber(docNumber)
                        .build());
                    rewardTransaction.setInvoiceUploadDate(LocalDateTime.now());
                    rewardTransaction.setUpdateDate(LocalDateTime.now());

                    String oldBatchId = rewardTransaction.getRewardBatchId();
                    YearMonth currentMonth = YearMonth.now();
                    PosType posType = rewardTransaction.getPointOfSaleType();
                    String businessName = rewardTransaction.getBusinessName();

                    String initiativeId = rewardTransaction.getInitiatives().get(0);

                    long accruedRewardCents = rewardTransaction.getRewards()
                        .get(initiativeId)
                        .getAccruedRewardCents();

                    return rewardBatchService.findOrCreateBatch(
                            merchantId,
                            posType,
                            currentMonth.toString(),
                            businessName
                        )
                        .flatMap(newRewardBatch -> {
                          if (newRewardBatch.getId().equals(oldBatchId)) {
                            return rewardTransactionRepository.save(rewardTransaction).then();
                          }
                          return rewardBatchService.decrementTotals(oldBatchId, accruedRewardCents)
                              .then(rewardBatchService.incrementTotals(newRewardBatch.getId(),
                                  accruedRewardCents))
                              .then(Mono.fromRunnable(() -> {
                                rewardTransaction.setRewardBatchId(newRewardBatch.getId());
                                rewardTransaction.setUpdateDate(LocalDateTime.now());
                              }))
                              .then(rewardTransactionRepository.save(rewardTransaction))
                              .then();
                        });
                  }));
            }));
          });
    } catch (Exception e) {
      throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
          ExceptionConstants.ExceptionCode.GENERIC_ERROR, "Error uploading invoice file", e);
    }
  }
}
