package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionKafkaMapper;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.notifier.TransactionNotifierService;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
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

    private static final DateTimeFormatter BATCH_MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM", Locale.ITALIAN);

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
      return rewardTransactionRepository.findTransaction(merchantId, pointOfSaleId, transactionId)
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
                                               String pointOfSaleId, FilePart file, String docNumber) {

        Utilities.checkFileExtensionOrThrow(file);

        return rewardTransactionRepository.findTransactionForUpdateInvoice(merchantId, pointOfSaleId, transactionId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
                .flatMap(trx -> {

                    String oldBatchId = trx.getRewardBatchId();
                    if (oldBatchId == null) {
                        return Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND));
                    }

                    // 1) batch deve essere EVALUATING
                    return rewardBatchRepository.findRewardBatchById(oldBatchId)
                            .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND)))
                            .flatMap(oldBatch -> {
                                if (!EVALUATING.equals(oldBatch.getStatus()) && !CREATED.equals(oldBatch.getStatus())) {
                                    return Mono.error(new ClientExceptionWithBody(
                                            HttpStatus.BAD_REQUEST,
                                            REWARD_BATCH_STATUS_NOT_ALLOWED,
                                            ERROR_MESSAGE_REWARD_BATCH_STATUS_NOT_ALLOWED
                                    ));
                                }

                                // 2) trx batch status deve essere != APPROVED
                                RewardBatchTrxStatus batchTrxStatus = trx.getRewardBatchTrxStatus(); // <-- assicurati che esista
                                if (batchTrxStatus == RewardBatchTrxStatus.APPROVED) {
                                    return Mono.error(new ClientExceptionWithBody(
                                            HttpStatus.BAD_REQUEST,
                                            TRANSACTION_STATUS_NOT_ALLOWED,
                                            TRANSACTION_NOT_STATUS_APPROVED
                                    ));
                                }

                                // 3) update file + campi fattura
                                InvoiceData oldDocumentData = validateTransactionData(trx, merchantId, pointOfSaleId);

                                return replaceInvoiceFile(file, oldDocumentData, merchantId, pointOfSaleId, transactionId)
                                        .then(Mono.defer(() -> {
                                            trx.setInvoiceData(InvoiceData.builder()
                                                    .filename(file.filename())
                                                    .docNumber(docNumber)
                                                    .build());
                                            trx.setInvoiceUploadDate(LocalDateTime.now());
                                            trx.setUpdateDate(LocalDateTime.now());

                                            // salva i campi fattura
                                            return rewardTransactionRepository.save(trx);
                                        }))
                                        // 4) DOPO update fattura -> sospendi (se non già sospesa)
                                        .flatMap(savedTrx -> {
                                            //Se CREATED non sospendere
                                            if (CREATED.equals(oldBatch.getStatus())) {
                                                return Mono.just(savedTrx);
                                            }
                                            if (savedTrx.getRewardBatchTrxStatus() == RewardBatchTrxStatus.SUSPENDED) {
                                                return Mono.just(savedTrx);
                                            }

                                            TransactionsRequest req = TransactionsRequest.builder()
                                                    .transactionIds(List.of(savedTrx.getId()))
                                                    .reason(savedTrx.getRejectionReasons() != null ? savedTrx.getRejectionReasons().toString() : null)
                                                    .build();


                                            return rewardBatchService
                                                    .suspendTransactions(oldBatchId, savedTrx.getInitiativeId(), req)
                                                    .then(rewardTransactionRepository.findTransactionForUpdateInvoice(merchantId, pointOfSaleId, transactionId))
                                                    .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)));
                                        })
                                        // 5) sposta batch al mese corrente + contatori suspended
                                        .flatMap(suspendedTrx -> {

                                            YearMonth currentMonth = YearMonth.now();
                                            PosType posType = suspendedTrx.getPointOfSaleType();
                                            String businessName = suspendedTrx.getBusinessName();

                                            String initiativeId = suspendedTrx.getInitiatives().getFirst();
                                            long accruedRewardCents = suspendedTrx.getRewards().get(initiativeId).getAccruedRewardCents();

                                            return rewardBatchService.findOrCreateBatch(
                                                            merchantId,
                                                            posType,
                                                            currentMonth.toString(),
                                                            businessName
                                                    )
                                                    .flatMap(newBatch -> {
                                                        if (newBatch.getId().equals(oldBatchId)) {
                                                            // batch già corretto, salvo solo eventuali updateDate già fatto
                                                            return rewardTransactionRepository.save(suspendedTrx).then();
                                                        }
                                                        Mono<Void> moveCounters;

                                                        if (CREATED.equals(oldBatch.getStatus())) {
                                                            //LEGACY: solo contatori base
                                                            moveCounters = rewardBatchService.decrementTotals(oldBatchId, accruedRewardCents)
                                                                    .then(rewardBatchService.incrementTotals(newBatch.getId(), accruedRewardCents))
                                                                    .then();
                                                        } else {
                                                            //EVALUATING: contatori "suspend" + elaborated tra lotti
                                                            moveCounters = rewardBatchService
                                                                    .moveSuspendToNewBatch(oldBatchId, newBatch.getId(), accruedRewardCents)
                                                                    .then();
                                                        }

                                                        return moveCounters
                                                                .then(Mono.fromRunnable(() -> {

                                                                    suspendedTrx.setRewardBatchId(newBatch.getId());
                                                                    suspendedTrx.setUpdateDate(LocalDateTime.now());

                                                                    if (!CREATED.equals(oldBatch.getStatus())) {
                                                                        updateLastMonthElaboratedOnBatchMove(
                                                                                suspendedTrx,
                                                                                oldBatch.getMonth(),
                                                                                newBatch.getMonth()
                                                                        );
                                                                    }
                                                                }))
                                                                .then(rewardTransactionRepository.save(suspendedTrx))
                                                                .then();
                                                    });
                                        })
                                        .then();
                            });
                });
    }

    private YearMonth getYearMonth (String yearMonthString){
        return YearMonth.parse(yearMonthString.toLowerCase(), BATCH_MONTH_FORMAT);
    }

    private void updateLastMonthElaboratedOnBatchMove(RewardTransaction trx, String oldBatchMonth, String newBatchMonth) {
        if (trx.getRewardBatchLastMonthElaborated() == null
                || getYearMonth(trx.getRewardBatchLastMonthElaborated()).isBefore(getYearMonth(newBatchMonth))) {
            trx.setRewardBatchLastMonthElaborated(oldBatchMonth);
        }
    }


    public Mono<Void> reversalTransaction(
            String transactionId,
            String merchantId,
            String pointOfSaleId,
            FilePart file,
            String docNumber
    ) {
        String sanitizedTransactionId = Utilities.sanitizeString(transactionId);
        String sanitizedPointOfSaleId = Utilities.sanitizeString(pointOfSaleId);
        String sanitizedMerchantId = Utilities.sanitizeString(merchantId);
        String sanitizedDocNumber = Utilities.sanitizeString(docNumber);

        Utilities.checkFileExtensionOrThrow(file);

        log.info("[REVERSAL-TRANSACTION-SERVICE] Start reversalTransaction transactionId={}, merchantId={}, posId={}, docNumber={}",
                sanitizedTransactionId, sanitizedMerchantId, sanitizedPointOfSaleId, sanitizedDocNumber);

        return rewardTransactionRepository.findTransaction(sanitizedMerchantId, sanitizedPointOfSaleId, sanitizedTransactionId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
                .doOnNext(rt -> log.info("[REVERSAL-TRANSACTION-SERVICE] Found transaction id={}, status={}, rewardBatchId={}",
                        rt.getId(), rt.getStatus(), rt.getRewardBatchId()))
                .flatMap(this::ensureTransactionIsInvoiced)
                .flatMap(rt -> {
                    String oldRewardBatchId = rt.getRewardBatchId();

                    return checkRewardBatchCreatedIfPresent(oldRewardBatchId)
                            .then(Mono.defer(() -> {
                                log.info("[REVERSAL-TRANSACTION-SERVICE] Uploading credit note BEFORE DB updates for trxId={}", rt.getId());

                                return uploadCreditNoteOrThrow(file, sanitizedMerchantId, sanitizedPointOfSaleId, sanitizedTransactionId, rt.getId())
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

                                            String initiativeId = rt.getInitiatives().getFirst();
                                            long accruedRewardCents = rt.getRewards().get(initiativeId).getAccruedRewardCents();

                                            Mono<Void> saveTransactionMono = rewardTransactionRepository.save(rt).then();

                                            Mono<Void> decrementRewardBatchMono =
                                                    oldRewardBatchId != null
                                                            ? rewardBatchRepository.decrementTotals(oldRewardBatchId, accruedRewardCents).then()
                                                            : Mono.empty();


                                            Mono<Void> sendToQueueMono = sendReversedInvoicedTransactionNotification(RewardTransactionKafkaMapper.toDto(rt));

                                            return saveTransactionMono
                                                    .then(decrementRewardBatchMono)
                                                    .then(sendToQueueMono);
                                        }));
                            }));
                })
                .doOnError(e -> log.error("[REVERSAL-TRANSACTION-SERVICE] Error during reversalTransaction [transactionId={}, merchantId={}]",
                        sanitizedTransactionId, sanitizedMerchantId, e))
                .then();
    }

    private Mono<RewardTransaction> ensureTransactionIsInvoiced(RewardTransaction rt) {
        if (!SyncTrxStatus.INVOICED.toString().equals(rt.getStatus())) {
            log.warn("[REVERSAL-TRANSACTION-SERVICE] Transaction id={} has invalid status={} (expected INVOICED)",
                    rt.getId(), rt.getStatus());
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    GENERIC_ERROR,
                    TRANSACTION_NOT_STATUS_INVOICED
            ));
        }
        return Mono.just(rt);
    }

    private Mono<Void> checkRewardBatchCreatedIfPresent(String rewardBatchId) {
        if (rewardBatchId == null) {
            return Mono.empty();
        }

        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND)))
                .doOnNext(rb -> log.info("[REVERSAL-TRANSACTION-SERVICE] Found reward batch id={} with status={}",
                        rb.getId(), rb.getStatus()))
                .flatMap(rb -> {
                    if (!CREATED.equals(rb.getStatus())) {
                        log.warn("[REVERSAL-TRANSACTION-SERVICE] Reward batch id={} not in CREATED status={}",
                                rb.getId(), rb.getStatus());
                        return Mono.error(new ClientExceptionWithBody(
                                HttpStatus.BAD_REQUEST,
                                REWARD_BATCH_ALREADY_SENT,
                                ERROR_MESSAGE_REWARD_BATCH_ALREADY_SENT
                        ));
                    }
                    return Mono.<Void>empty();
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
