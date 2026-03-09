package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionKafkaMapper;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.notifier.TransactionNotifierService;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.reversal.ReversalPolicy;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.YearMonth;
import java.util.List;
import java.util.Objects;

import static it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.GENERIC_ERROR;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;


@Service
@Slf4j
public class PointOfSaleTransactionServiceImpl implements PointOfSaleTransactionService {

  private final UserRestClient userRestClient;
  private final RewardTransactionRepository rewardTransactionRepository;
  private final InvoiceStorageClient invoiceStorageClient;
  private final RewardBatchService rewardBatchService;
  private final RewardBatchRepository rewardBatchRepository;
  private final TransactionErrorNotifierService transactionErrorNotifierService;
  private final TransactionNotifierService transactionNotifierService;

  protected PointOfSaleTransactionServiceImpl(
          UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository, InvoiceStorageClient invoiceStorageClient, RewardBatchService rewardBatchService,
      RewardBatchRepository rewardBatchRepository,
      TransactionErrorNotifierService transactionErrorNotifierService,
      TransactionNotifierService transactionNotifierService) {
    this.userRestClient = userRestClient;
    this.rewardTransactionRepository = rewardTransactionRepository;
    this.invoiceStorageClient = invoiceStorageClient;
    this.rewardBatchService = rewardBatchService;
    this.rewardBatchRepository = rewardBatchRepository;
    this.transactionErrorNotifierService = transactionErrorNotifierService;
    this.transactionNotifierService = transactionNotifierService;
  }

    @Override
    public Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId,
                                                                    String initiativeId,
                                                                    String pointOfSaleId,
                                                                    String productGtin,
                                                                    TrxFiltersDTO filters,
                                                                    Pageable pageable) {

        filters.setMerchantId(merchantId);
        filters.setInitiativeId(initiativeId);


        if (StringUtils.isNotBlank(filters.getFiscalCode())) {
            return userRestClient.retrieveFiscalCodeInfo(filters.getFiscalCode())
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
      return rewardTransactionRepository.findTransaction(merchantId, transactionId)
              .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
              .handle((rewardTransaction, sink) -> {
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
                    sink.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE));
                    return;
                }

                if (documentData == null || documentData.getFilename() == null) {
                    sink.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE));
                    return;
                }

                String filename = documentData.getFilename();

                String blobPath = String.format("invoices/merchant/%s/pos/%s/transaction/%s/%s/%s",
                    merchantId, pointOfSaleId, transactionId, typeFolder, filename);

                  sink.next(DownloadInvoiceResponseDTO.builder()
                          .invoiceUrl(invoiceStorageClient.getFileSignedUrl(blobPath))
                          .build());
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
                                               FilePart file, String docNumber) {

        log.info("[UPDATE_INVOICE_FILE_SERVICE] - [updateInvoiceTransaction] - start | trxId={} merchantId={} docNumber={} filename={}",
                Utilities.sanitizeString(transactionId), Utilities.sanitizeString(merchantId), Utilities.sanitizeString(docNumber), file != null ? Utilities.sanitizeString(file.filename()) : null);

        Utilities.checkFileExtensionOrThrow(file);

        return rewardTransactionRepository
                .findTransaction(merchantId, transactionId)
                .switchIfEmpty(Mono.defer(() -> Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE))))
                .flatMap(trx -> validateBatchAndUpdateInvoiceFlow(trx, merchantId, transactionId, file, docNumber));
    }

    private Mono<Void> validateBatchAndUpdateInvoiceFlow(RewardTransaction trx,
                                                         String merchantId,
                                                         String transactionId,
                                                         FilePart file,
                                                         String docNumber) {

        String oldBatchId = requireRewardBatchId(trx);

        return rewardBatchRepository.findRewardBatchById(oldBatchId)
                .switchIfEmpty(Mono.defer(() -> Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND))))
                .flatMap(oldBatch -> {
                    validateOldBatchStatusAllowed(oldBatch);
                    validateTrxBatchStatusNotApproved(trx);
                    transactionIsInvoicedOrRewarded(trx);

                    return updateInvoiceFileAndFields(trx, merchantId, trx.getPointOfSaleId(), transactionId, file, docNumber)
                            .flatMap(savedTrx -> suspendAndMoveTransaction(savedTrx, oldBatch))
                            .then();
                });
    }

    private String requireRewardBatchId(RewardTransaction trx) {
        String oldBatchId = trx.getRewardBatchId();
        if (oldBatchId == null) {
            throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND);
        }

        return oldBatchId;
    }

    private void validateOldBatchStatusAllowed(RewardBatch oldBatch) {
        if (!EVALUATING.equals(oldBatch.getStatus())
                && !CREATED.equals(oldBatch.getStatus())
                && !APPROVED.equals(oldBatch.getStatus())) {
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    REWARD_BATCH_STATUS_NOT_ALLOWED,
                    ERROR_MESSAGE_REWARD_BATCH_STATUS_NOT_ALLOWED
            );
        }
}

    private void validateTrxBatchStatusNotApproved(RewardTransaction trx) {
        if (trx.getRewardBatchTrxStatus() == RewardBatchTrxStatus.APPROVED) {
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    TRANSACTION_STATUS_NOT_ALLOWED,
                    TRANSACTION_NOT_STATUS_APPROVED
            );
        }
    }

    private Mono<RewardTransaction> updateInvoiceFileAndFields(RewardTransaction trx,
                                                               String merchantId,
                                                               String pointOfSaleId,
                                                               String transactionId,
                                                               FilePart file,
                                                               String docNumber) {

        InvoiceData oldDocumentData = validateTransactionData(trx, merchantId, pointOfSaleId);

        return replaceInvoiceFile(file, oldDocumentData, merchantId, pointOfSaleId, transactionId)
                .then(Mono.defer(() -> {
                    trx.setInvoiceData(InvoiceData.builder()
                            .filename(file.filename())
                            .docNumber(docNumber)
                            .build());
                    trx.setInvoiceUploadDate(LocalDateTime.now());
                    trx.setUpdateDate(LocalDateTime.now());
                    return rewardTransactionRepository.save(trx);
                }));
    }

    private Mono<RewardBatch> findOrCreateTargetBatch(RewardTransaction oldTransaction,
                                                      RewardBatch oldBatch) {

        PosType posType = oldTransaction.getPointOfSaleType();
        String businessName = oldTransaction.getBusinessName();

        YearMonth currentMonth = YearMonth.now();
        YearMonth oldMonth = YearMonth.parse(oldBatch.getMonth());
        YearMonth targetMonth = oldMonth.isAfter(currentMonth) ? oldMonth : currentMonth;

        log.info("[UPDATE_INVOICE_FILE_SERVICE] - [findOrCreateTargetBatch] - start | oldBatchId={} trxId={} targetMonth={}",
                Utilities.sanitizeString(oldBatch.getId()), Utilities.sanitizeString(oldTransaction.getId()), targetMonth);

        return rewardBatchService.findOrCreateBatch(oldBatch.getMerchantId(), posType, targetMonth.toString(), businessName);
    }

    private Mono<RewardTransaction> suspendAndMoveTransaction(
            RewardTransaction oldTransaction, RewardBatch oldBatch) {

        if (CREATED.equals(oldBatch.getStatus())) {
            log.info("[UPDATE_INVOICE_FILE_SERVICE] - [suspendAndMoveTransaction] - end success | no-move (old batch CREATED)");
            return Mono.just(oldTransaction);
        }

        long accruedRewardCents =
                oldTransaction
                        .getRewards()
                        .get(oldTransaction.getInitiatives().getFirst())
                        .getAccruedRewardCents();

        boolean wasSuspended = oldTransaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.SUSPENDED;
        boolean wasRejected = oldTransaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.REJECTED;

        BatchCountersDTO oldBatchCounter;
        BatchCountersDTO newBatchCounter;

        if (wasSuspended) {
            oldBatchCounter = BatchCountersDTO.newBatch()
                    .decrementNumberOfTransactions()
                    .decrementTrxElaborated();

            newBatchCounter = BatchCountersDTO.newBatch()
                    .incrementInitialAmountCents(accruedRewardCents)
                    .incrementNumberOfTransactions(1L)
                    .incrementTrxSuspended(1L)
                    .incrementSuspendedAmountCents(accruedRewardCents)
                    .incrementTrxElaborated(1L);
        } else {
            oldBatchCounter = BatchCountersDTO.newBatch()
                    .decrementNumberOfTransactions()
                    .decrementTrxElaborated(wasRejected ? 1L : 0L);

            newBatchCounter = BatchCountersDTO.newBatch()
                    .incrementInitialAmountCents(accruedRewardCents)
                    .incrementNumberOfTransactions(1L)
                    .incrementTrxSuspended(1L)
                    .incrementSuspendedAmountCents(accruedRewardCents)
                    .incrementTrxElaborated(1L);
        }

        return findOrCreateTargetBatch(oldTransaction, oldBatch)
                .flatMap(newBatch -> {
                    log.info("[UPDATE_INVOICE_FILE_SERVICE] - [suspendAndMoveTransaction] - moving trx | trxId={} fromBatchId={} toBatchId={} oldCounters={} newCounters={}",
                            Utilities.sanitizeString(oldTransaction.getId()), Utilities.sanitizeString(oldBatch.getId()), Utilities.sanitizeString(newBatch.getId()),
                            oldBatchCounter, newBatchCounter);

                    oldTransaction.setStatus(SyncTrxStatus.INVOICED.name());
                    oldTransaction.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
                    oldTransaction.setRewardBatchId(newBatch.getId());
                    oldTransaction.setUpdateDate(LocalDateTime.now());

                    return rewardTransactionRepository.save(oldTransaction)
                            .then(rewardBatchRepository.updateTotals(oldBatch.getId(), oldBatchCounter))
                            .then(rewardBatchRepository.updateTotals(newBatch.getId(), newBatchCounter))
                            .thenReturn(oldTransaction);
                });
    }

    @Override
    public Mono<Void> reversalTransaction(
            String transactionId,
            String merchantId,
            FilePart file,
            String docNumber,
            ReversalPolicy policy
    ) {
        String sanitizedTransactionId = Utilities.sanitizeString(transactionId);
        String sanitizedMerchantId = Utilities.sanitizeString(merchantId);
        String sanitizedDocNumber = Utilities.sanitizeString(docNumber);

        Utilities.checkFileExtensionOrThrow(file);

        log.info("[REVERSAL-TRANSACTION-SERVICE] Start reversalTransaction transactionId={}, merchantId={}, docNumber={}",
                sanitizedTransactionId, sanitizedMerchantId, sanitizedDocNumber);

        return rewardTransactionRepository.findTransaction(sanitizedMerchantId, sanitizedTransactionId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
                .doOnNext(rt -> log.info("[REVERSAL-TRANSACTION-SERVICE] Found transaction id={}, status={}, rewardBatchId={}",
                        rt.getId(), rt.getStatus(), rt.getRewardBatchId()))
                .flatMap(rt -> policy.validate(rt)) // TODO refactor this with following step
                .flatMap(rt ->
                        rt.getRewardBatchId() != null
                                ? rewardBatchAllowedStatus(rt)
                                : Mono.just(rt)
                )
                .flatMap(rt -> {
                    final String oldRewardBatchId = rt.getRewardBatchId();
                    final RewardBatchTrxStatus oldBatchTrxStatus = rt.getRewardBatchTrxStatus();
                    final boolean wasSuspended = RewardBatchTrxStatus.SUSPENDED.equals(oldBatchTrxStatus);
                    final boolean wasRejected = RewardBatchTrxStatus.REJECTED.equals(oldBatchTrxStatus);
                    String initiativeId = rt.getInitiatives().getFirst();
                    long accruedRewardCents = rt.getRewards().get(initiativeId).getAccruedRewardCents();

                    BatchCountersDTO counters = BatchCountersDTO.newBatch()
                            .decrementNumberOfTransactions()
                            .decrementInitialAmountCents(accruedRewardCents);
                    if (wasSuspended) {
                        counters.decrementSuspendedAmountCents(accruedRewardCents)
                                .decrementTrxSuspended()
                                .decrementTrxElaborated();
                    }
                    if (wasRejected) {
                        counters.decrementTrxRejected()
                                .decrementTrxElaborated();
                    }

                    return Mono.defer(() -> {
                                log.info("[REVERSAL-TRANSACTION-SERVICE] Uploading credit note BEFORE DB updates for trxId={}", rt.getId());

                                return uploadCreditNoteOrThrow(file, sanitizedMerchantId, rt.getPointOfSaleId(), sanitizedTransactionId, rt.getId())
                                        .then(Mono.defer(() -> {
                                            log.info("[REVERSAL-TRANSACTION-SERVICE] Upload OK. Applying DB updates for trxId={}", rt.getId());

                                            rt.setRewardBatchId(null);
                                            rt.setRewardBatchInclusionDate(null);
                                            rt.setRewardBatchTrxStatus(null);
                                            rt.setSamplingKey(0);

                                            rt.setStatus(SyncTrxStatus.REFUNDED.toString());
                                            rt.setUpdateDate(LocalDateTime.now());

                                            rt.setCreditNoteData(InvoiceData.builder()
                                                    .filename(file.filename())
                                                    .docNumber(sanitizedDocNumber)
                                                    .build());



                                            Mono<Void> saveTransactionMono = rewardTransactionRepository.save(rt).then();

                                            Mono<Void> updateBatchTotalsMono =
                                                    oldRewardBatchId != null
                                                            ? rewardBatchRepository.updateTotals(oldRewardBatchId, counters).then()
                                                            : Mono.empty();


                                            Mono<Void> sendToQueueMono = sendReversedInvoicedTransactionNotification(RewardTransactionKafkaMapper.toDto(rt));

                                            return saveTransactionMono
                                                    .then(updateBatchTotalsMono)
                                                    .then(sendToQueueMono);
                                        }));
                            });
                })
                .doOnError(e -> log.error("[REVERSAL-TRANSACTION-SERVICE] Error during reversalTransaction [transactionId={}, merchantId={}]",
                        sanitizedTransactionId, sanitizedMerchantId, e))
                .then();
    }

    private Mono<RewardTransaction> transactionIsInvoicedOrRewarded(RewardTransaction rt) {
        if (!SyncTrxStatus.INVOICED.toString().equals(rt.getStatus())
                && !SyncTrxStatus.REWARDED.toString().equals(rt.getStatus())) {
            log.warn("[REVERSAL-TRANSACTION-SERVICE] Transaction id={} has invalid status={} (expected INVOICED or REWARDED)",
                    rt.getId(), rt.getStatus());
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    GENERIC_ERROR,
                    TRANSACTION_NOT_STATUS_INVOICED_OR_REWARDED
            ));
        }
        return Mono.just(rt);
    }

    private Mono<RewardTransaction> rewardBatchAllowedStatus(RewardTransaction rt) {
        // TODO: this must be merged with policy logic
        return rewardBatchRepository.findById(rt.getRewardBatchId())
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("[REVERSAL-TRANSACTION-SERVICE] RewardBatch id={} not found",
                            rt.getRewardBatchId());
                    return Mono.error(new ClientExceptionWithBody(
                            HttpStatus.NOT_FOUND,
                            GENERIC_ERROR,
                            REWARD_BATCH_NOT_FOUND
                    ));
                }))
                .flatMap(rewardBatch -> {
                    String status = rewardBatch.getStatus().toString();

                    if (!RewardBatchStatus.APPROVED.toString().equals(status)
                            && !RewardBatchStatus.EVALUATING.toString().equals(status)
                            && !RewardBatchStatus.CREATED.toString().equals(status)) {

                        log.warn("[REVERSAL-TRANSACTION-SERVICE] RewardBatch id={} has invalid status={} (expected APPROVED, EVALUATING or CREATED)",
                                rewardBatch.getId(), status);

                        return Mono.error(new ClientExceptionWithBody(
                                HttpStatus.BAD_REQUEST,
                                GENERIC_ERROR,
                                REWARD_BATCH_STATUS_NOT_ALLOWED
                        ));
                    }
                    return Mono.just(rt);
                });
    }


    private Mono<Void> uploadCreditNoteOrThrow(
            FilePart file,
            String merchantId,
            String pointOfSaleId,
            String transactionId,
            String rtIdForLog
    ) {
        return addCreditNoteFile(file, merchantId, pointOfSaleId, transactionId)
                .doOnSuccess(v -> log.info("[REVERSAL-TRANSACTION-SERVICE] Credit note uploaded for trxId={}", rtIdForLog))
                .onErrorMap(IOException.class, e -> {
                    log.error("[REVERSAL-TRANSACTION-SERVICE] IOException uploading credit note trxId={}", rtIdForLog, e);
                    return new ClientExceptionWithBody(
                            HttpStatus.INTERNAL_SERVER_ERROR,
                            GENERIC_ERROR,
                            "Error uploading credit note file"
                    );
                })
                .onErrorMap(com.azure.identity.CredentialUnavailableException.class, e -> {
                    log.error("[REVERSAL-TRANSACTION-SERVICE] Azure credentials unavailable uploading credit note trxId={}", rtIdForLog, e);
                    return new ClientExceptionWithBody(
                            HttpStatus.INTERNAL_SERVER_ERROR,
                            GENERIC_ERROR,
                            "Azure credentials not available for credit note upload"
                    );
                })
                .onErrorMap(RuntimeException.class, e -> {
                    log.error("[REVERSAL-TRANSACTION-SERVICE] Generic error uploading credit note trxId={}", rtIdForLog, e);
                    return new ClientExceptionWithBody(
                            HttpStatus.INTERNAL_SERVER_ERROR,
                            GENERIC_ERROR,
                            "Error uploading credit note file"
                    );
                });
    }

    private Mono<Void> sendReversedInvoicedTransactionNotification(RewardTransactionKafkaDTO trx) {
    return Mono.fromRunnable(() -> {
          log.info(
              "[REVERSAL_INVOICED_TRANSACTION][SEND_NOTIFICATION] Sending Reverse Invoiced Transaction event to Notification: trxId {} - merchantId {}",
              trx.getId(), trx.getMerchantId());

          if (!transactionNotifierService.notify(trx, trx.getUserId())) {
            throw new IllegalStateException(
                "[TRANSACTION_REVERSAL_INVOICED_REQUEST] Something gone wrong while reversing Invoiced Transaction notify");
          }
        })
        .onErrorResume(e -> {
          log.error(
              "[UNEXPECTED_REVERSAL_INVOICED_ERROR][SEND_NOTIFICATION] An error has occurred and was not possible to notify it: trxId {} - merchantId {}",
              trx.getId(), trx.getUserId(), e);

          transactionErrorNotifierService.notifyTransactionOutcome(
              transactionNotifierService.buildMessage(trx, trx.getUserId()),
              "[REVERSAL_INVOICED_TRANSACTION_REQUEST] An error occurred while publishing the reversal invoiced result: trxId %s - merchantId %s".formatted(
                  trx.getId(), trx.getMerchantId()),
              true,
              e
          );

          return Mono.error(e);
        }).then();
  }

    private Mono<Void> replaceInvoiceFile(FilePart file,
      InvoiceData oldDocumentData,
      String merchantId,
      String pointOfSaleId,
      String transactionId) {

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
                ? Objects.requireNonNull(file.headers().getContentType()).toString()
                : null;
            invoiceStorageClient.upload(is, blobPath, contentType);
          }
          return Boolean.TRUE;
        }))
        .onErrorMap(IOException.class, e -> {
          log.error("Error uploading file to storage for transaction [{}]",
              Utilities.sanitizeString(transactionId), e);
          throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
              GENERIC_ERROR,
              "Error uploading invoice file", e);
        })
        .then();
  }

    Mono<Void> addCreditNoteFile(FilePart file,
                                 String merchantId,
                                 String pointOfSaleId,
                                 String transactionId) {

        String blobPath = String.format(
                "invoices/merchant/%s/pos/%s/transaction/%s/creditNote/%s",
                merchantId, pointOfSaleId, transactionId, file.filename());

        Path tempPath = Paths.get(System.getProperty("java.io.tmpdir"), file.filename());

        return file.transferTo(tempPath)
                .then(Mono.fromCallable(() -> {

                    try (InputStream is = Files.newInputStream(tempPath)) {
                        String contentType = file.headers().getContentType() != null
                                ? Objects.requireNonNull(file.headers().getContentType()).toString()
                                : null;
                        invoiceStorageClient.upload(is, blobPath, contentType);
                    }
                    return Boolean.TRUE;
                }))
                .onErrorMap(IOException.class, e -> {
                    log.error("Error uploading file to storage for transaction [{}]",
                            Utilities.sanitizeString(transactionId), e);
                    throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
                            GENERIC_ERROR,
                            "Error uploading credit note file", e);
                })
                .then();
    }

  private InvoiceData validateTransactionData(RewardTransaction rewardTransaction, String merchantId, String pointOfSaleId) {

    if (!rewardTransaction.getMerchantId().equals(merchantId)) {
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
          GENERIC_ERROR,
          "The merchant with id [%s] associated to the transaction is not equal to the merchant with id [%s]".formatted(
              rewardTransaction.getMerchantId(), merchantId));
    }

    if (!rewardTransaction.getPointOfSaleId().equals(pointOfSaleId)) {
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
          GENERIC_ERROR,
          "The pointOfSaleId with id [%s] associated to the transaction is not equal to the pointOfSaleId with id [%s]".formatted(
              rewardTransaction.getPointOfSaleId(), pointOfSaleId));
    }

    return rewardTransaction.getInvoiceData();
  }

    @Override
    public Mono<List<FranchisePointOfSaleDTO>> getDistinctFranchiseAndPosByRewardBatchId(String rewardBatchId) {
      log.info("[POINT_OF_SALE_TRANSACTION_SERVICE] - Get point of sale for reward batch id [{}]", Utilities.sanitizeString(rewardBatchId));
        return rewardTransactionRepository
                .findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId)
                .collectList();
    }

}
