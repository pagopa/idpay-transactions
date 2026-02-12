package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionKafkaMapper;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
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

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-TRANSACTION] - Start. transactionId={}, merchantId={}, pointOfSaleId={}, docNumber={}, filename={}",
                transactionId, merchantId, pointOfSaleId, docNumber, file != null ? file.filename() : null);

        Utilities.checkFileExtensionOrThrow(file);

        return rewardTransactionRepository
                .findTransactionForUpdateInvoice(merchantId, pointOfSaleId, transactionId)
                .doOnSubscribe(s -> log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-TRANSACTION] - Searching transaction for invoice update. transactionId={}, merchantId={}, pointOfSaleId={}",
                        transactionId, merchantId, pointOfSaleId))
                .doOnNext(trx -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-TRANSACTION] - Transaction found. trxId={}, rewardBatchId={}, rewardBatchTrxStatus={}",
                        trx.getId(), trx.getRewardBatchId(), trx.getRewardBatchTrxStatus()))
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
                .flatMap(trx -> validateBatchAndUpdateInvoiceFlow(trx, merchantId, pointOfSaleId, transactionId, file, docNumber))
                .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-TRANSACTION] - Completed successfully. transactionId={}, merchantId={}, pointOfSaleId={}",
                        transactionId, merchantId, pointOfSaleId))
                .doOnError(e -> log.error("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-TRANSACTION] - Failed. transactionId={}, merchantId={}, pointOfSaleId={}, error={}",
                        transactionId, merchantId, pointOfSaleId, e.toString(), e));
    }

    private Mono<Void> validateBatchAndUpdateInvoiceFlow(RewardTransaction trx,
                                                         String merchantId,
                                                         String pointOfSaleId,
                                                         String transactionId,
                                                         FilePart file,
                                                         String docNumber) {

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Start. trxId={}, merchantId={}, pointOfSaleId={}, transactionId={}",
                trx != null ? trx.getId() : null, merchantId, pointOfSaleId, transactionId);

        String oldBatchId = requireRewardBatchId(trx);

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Required rewardBatchId resolved. oldBatchId={}, trxId={}",
                oldBatchId, trx.getId());

        return rewardBatchRepository.findRewardBatchById(oldBatchId)
                .doOnSubscribe(s -> log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Loading old batch. oldBatchId={}", oldBatchId))
                .doOnNext(oldBatch -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Old batch found. oldBatchId={}, status={}, month={}",
                        oldBatch.getId(), oldBatch.getStatus(), oldBatch.getMonth()))
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND)))
                .flatMap(oldBatch -> {
                    log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Validating old batch status and trx batch status. oldBatchId={}, trxId={}",
                            oldBatchId, trx.getId());

                    validateOldBatchStatusAllowed(oldBatch);
                    validateTrxBatchStatusNotApproved(trx);

                    log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Validation passed. Proceeding with invoice update flow. trxId={}, oldBatchId={}",
                            trx.getId(), oldBatchId);

                    return updateInvoiceFileAndFields(trx, merchantId, pointOfSaleId, transactionId, file, docNumber)
                            .doOnSubscribe(s -> log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Updating invoice file and fields. trxId={}", trx.getId()))
                            .doOnNext(savedTrx -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Invoice updated and trx saved. trxId={}, invoiceFilename={}, docNumber={}",
                                    savedTrx.getId(),
                                    savedTrx.getInvoiceData() != null ? savedTrx.getInvoiceData().getFilename() : null,
                                    savedTrx.getInvoiceData() != null ? savedTrx.getInvoiceData().getDocNumber() : null))
                            .flatMap(savedTrx -> suspendIfNeeded(savedTrx, oldBatch, oldBatchId, merchantId, pointOfSaleId, transactionId))
                            .doOnNext(suspendedTrx -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Suspend step completed. trxId={}, rewardBatchTrxStatus={}",
                                    suspendedTrx.getId(), suspendedTrx.getRewardBatchTrxStatus()))
                            .flatMap(suspendedTrx -> moveToCurrentMonthBatchAndUpdateCounters(suspendedTrx, oldBatch, oldBatchId, merchantId))
                            .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Flow completed. trxId={}, oldBatchId={}",
                                    trx.getId(), oldBatchId))
                            .then();
                })
                .doOnError(e -> log.error("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-BATCH-AND-UPDATE-INVOICE-FLOW] - Failed. trxId={}, oldBatchId={}, error={}",
                        trx != null ? trx.getId() : null, oldBatchId, e.toString(), e));
    }

    private String requireRewardBatchId(RewardTransaction trx) {
        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [REQUIRE-REWARD-BATCH-ID] - Checking rewardBatchId. trxId={}, rewardBatchId={}",
                trx != null ? trx.getId() : null, trx != null ? trx.getRewardBatchId() : null);

        String oldBatchId = trx.getRewardBatchId();
        if (oldBatchId == null) {
            log.warn("[POINT-OF-SALE-TRANSACTION-SERVICE] - [REQUIRE-REWARD-BATCH-ID] - Missing rewardBatchId on transaction. trxId={}",
                    trx != null ? trx.getId() : null);
            throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND);
        }

        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [REQUIRE-REWARD-BATCH-ID] - rewardBatchId present. oldBatchId={}, trxId={}",
                oldBatchId, trx.getId());

        return oldBatchId;
    }

    /** (1) batch deve essere EVALUATING o CREATED */
    private void validateOldBatchStatusAllowed(RewardBatch oldBatch) {
        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-OLD-BATCH-STATUS-ALLOWED] - Validating old batch status. batchId={}, status={}",
                oldBatch != null ? oldBatch.getId() : null, oldBatch != null ? oldBatch.getStatus() : null);

        if (!EVALUATING.equals(oldBatch.getStatus()) && !CREATED.equals(oldBatch.getStatus())) {
            log.warn("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-OLD-BATCH-STATUS-ALLOWED] - Batch status not allowed. batchId={}, status={}",
                    oldBatch.getId(), oldBatch.getStatus());
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    REWARD_BATCH_STATUS_NOT_ALLOWED,
                    ERROR_MESSAGE_REWARD_BATCH_STATUS_NOT_ALLOWED
            );
        }

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-OLD-BATCH-STATUS-ALLOWED] - Batch status allowed. batchId={}, status={}",
                oldBatch.getId(), oldBatch.getStatus());
    }

    /** (2) trx batch status deve essere != APPROVED */
    private void validateTrxBatchStatusNotApproved(RewardTransaction trx) {
        RewardBatchTrxStatus batchTrxStatus = trx.getRewardBatchTrxStatus();

        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-TRX-BATCH-STATUS-NOT-APPROVED] - Validating trx batch status. trxId={}, rewardBatchTrxStatus={}",
                trx != null ? trx.getId() : null, batchTrxStatus);

        if (batchTrxStatus == RewardBatchTrxStatus.APPROVED) {
            log.warn("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-TRX-BATCH-STATUS-NOT-APPROVED] - Transaction status not allowed (APPROVED). trxId={}",
                    trx.getId());
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    TRANSACTION_STATUS_NOT_ALLOWED,
                    TRANSACTION_NOT_STATUS_APPROVED
            );
        }

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [VALIDATE-TRX-BATCH-STATUS-NOT-APPROVED] - Transaction status allowed. trxId={}, rewardBatchTrxStatus={}",
                trx.getId(), batchTrxStatus);
    }

    /** (3) replace file + update campi fattura + save trx */
    private Mono<RewardTransaction> updateInvoiceFileAndFields(RewardTransaction trx,
                                                               String merchantId,
                                                               String pointOfSaleId,
                                                               String transactionId,
                                                               FilePart file,
                                                               String docNumber) {

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Start. trxId={}, merchantId={}, pointOfSaleId={}, transactionId={}, docNumber={}, filename={}",
                trx != null ? trx.getId() : null, merchantId, pointOfSaleId, transactionId, docNumber, file != null ? file.filename() : null);

        InvoiceData oldDocumentData = validateTransactionData(trx, merchantId, pointOfSaleId);

        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Old invoice data validated. trxId={}, oldFilename={}, oldDocNumber={}",
                trx.getId(),
                oldDocumentData != null ? oldDocumentData.getFilename() : null,
                oldDocumentData != null ? oldDocumentData.getDocNumber() : null);

        return replaceInvoiceFile(file, oldDocumentData, merchantId, pointOfSaleId, transactionId)
                .doOnSubscribe(s -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Replacing invoice file. trxId={}, transactionId={}, filename={}",
                        trx.getId(), transactionId, file != null ? file.filename() : null))
                .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Invoice file replaced. trxId={}, transactionId={}",
                        trx.getId(), transactionId))
                .then(Mono.defer(() -> {
                    log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Updating invoice fields on transaction. trxId={}", trx.getId());

                    trx.setInvoiceData(InvoiceData.builder()
                            .filename(file.filename())
                            .docNumber(docNumber)
                            .build());
                    trx.setInvoiceUploadDate(LocalDateTime.now());
                    trx.setUpdateDate(LocalDateTime.now());

                    return rewardTransactionRepository.save(trx)
                            .doOnSuccess(saved -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Transaction saved after invoice update. trxId={}, invoiceFilename={}, docNumber={}",
                                    saved.getId(),
                                    saved.getInvoiceData() != null ? saved.getInvoiceData().getFilename() : null,
                                    saved.getInvoiceData() != null ? saved.getInvoiceData().getDocNumber() : null));
                }))
                .doOnError(e -> log.error("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-INVOICE-FILE-AND-FIELDS] - Failed. trxId={}, error={}",
                        trx != null ? trx.getId() : null, e.toString(), e));
    }

    /** (4) DOPO update fattura -> sospende (se non già sospesa). Se batch CREATED non sospendere. */
    private Mono<RewardTransaction> suspendIfNeeded(RewardTransaction savedTrx,
                                                    RewardBatch oldBatch,
                                                    String oldBatchId,
                                                    String merchantId,
                                                    String pointOfSaleId,
                                                    String transactionId) {

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - Start. trxId={}, oldBatchId={}, oldBatchStatus={}, rewardBatchTrxStatus={}",
                savedTrx != null ? savedTrx.getId() : null,
                oldBatchId,
                oldBatch != null ? oldBatch.getStatus() : null,
                savedTrx != null ? savedTrx.getRewardBatchTrxStatus() : null);

        // Se CREATED non sospendere
        if (CREATED.equals(oldBatch.getStatus())) {
            log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - Skip suspend because old batch is CREATED. trxId={}, oldBatchId={}",
                    savedTrx.getId(), oldBatchId);
            return Mono.just(savedTrx);
        }

        // Se già suspended non fare nulla
        if (savedTrx.getRewardBatchTrxStatus() == RewardBatchTrxStatus.SUSPENDED) {
            log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - Skip suspend because transaction is already SUSPENDED. trxId={}, oldBatchId={}",
                    savedTrx.getId(), oldBatchId);
            return Mono.just(savedTrx);
        }

        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of(savedTrx.getId()))
                .reason(savedTrx.getRejectionReasons() != null ? savedTrx.getRejectionReasons().toString() : null)
                .build();

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - Suspending transaction. trxId={}, oldBatchId={}, initiativeId={}, reasonPresent={}",
                savedTrx.getId(), oldBatchId, savedTrx.getInitiativeId(), req.getReason() != null);

        return rewardBatchService
                .suspendTransactions(oldBatchId, savedTrx.getInitiativeId(), req)
                .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - SuspendTransactions executed successfully. trxId={}, oldBatchId={}",
                        savedTrx.getId(), oldBatchId))
                .then(rewardTransactionRepository.findTransactionForUpdateInvoice(merchantId, pointOfSaleId, transactionId))
                .doOnNext(trx -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - Reloaded transaction after suspend. trxId={}, rewardBatchTrxStatus={}",
                        trx.getId(), trx.getRewardBatchTrxStatus()))
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE)))
                .doOnError(e -> log.error("[POINT-OF-SALE-TRANSACTION-SERVICE] - [SUSPEND-IF-NEEDED] - Failed. trxId={}, oldBatchId={}, error={}",
                        savedTrx != null ? savedTrx.getId() : null, oldBatchId, e.toString(), e));
    }

    /** (5) sposta trx al batch del mese corrente + contatori suspended */
    private Mono<Void> moveToCurrentMonthBatchAndUpdateCounters(
            RewardTransaction suspendedTrx,
            RewardBatch oldBatch,
            String oldBatchId,
            String merchantId
    ) {

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Start. trxId={}, oldBatchId={}, oldBatchStatus={}, oldBatchMonth={}, merchantId={}",
                suspendedTrx != null ? suspendedTrx.getId() : null,
                oldBatchId,
                oldBatch != null ? oldBatch.getStatus() : null,
                oldBatch != null ? oldBatch.getMonth() : null,
                merchantId);

        PosType posType = suspendedTrx.getPointOfSaleType();
        String businessName = suspendedTrx.getBusinessName();

        YearMonth now = YearMonth.now();
        YearMonth originalBatchMonth = YearMonth.parse(oldBatch.getMonth());
        YearMonth targetMonth = originalBatchMonth.isAfter(now) ? originalBatchMonth : now;

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Computed targetMonth. now={}, originalBatchMonth={}, targetMonth={}",
                now, originalBatchMonth, targetMonth);

        String initiativeId = suspendedTrx.getInitiatives().getFirst();
        long accruedRewardCents = suspendedTrx.getRewards().get(initiativeId).getAccruedRewardCents();

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Accrued reward cents resolved. trxId={}, initiativeId={}, accruedRewardCents={}",
                suspendedTrx.getId(), initiativeId, accruedRewardCents);

        return rewardBatchService.findOrCreateBatch(merchantId, posType, targetMonth.toString(), businessName)
                .doOnSubscribe(s -> log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Finding/creating batch for targetMonth. targetMonth={}, merchantId={}, posType={}, businessName={}",
                        targetMonth, merchantId, posType, businessName))
                .flatMap(batchFound -> {

                    log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Batch found for targetMonth. batchId={}, status={}, month={}",
                            batchFound.getId(), batchFound.getStatus(), batchFound.getMonth());

                    // Se sto puntando al mese corrente ma il batch è EVALUATING -> vado al mese successivo
                    if (targetMonth.equals(now) && batchFound.getStatus() == EVALUATING) {
                        YearMonth nextMonth = now.plusMonths(1);

                        log.warn("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Batch for current month is EVALUATING, switching to nextMonth. currentMonth={}, nextMonth={}, batchId={}",
                                now, nextMonth, batchFound.getId());

                        return rewardBatchService.findOrCreateBatch(merchantId, posType, nextMonth.toString(), businessName)
                                .doOnNext(b -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Batch found for nextMonth. batchId={}, status={}, month={}",
                                        b.getId(), b.getStatus(), b.getMonth()));
                    }

                    return Mono.just(batchFound);
                })
                .flatMap(newBatch -> {

                    log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Destination batch resolved. newBatchId={}, status={}, month={}, oldBatchId={}",
                            newBatch.getId(), newBatch.getStatus(), newBatch.getMonth(), oldBatchId);

                    if (newBatch.getId().equals(oldBatchId) && newBatch.getStatus() == CREATED) {
                        // batch già corretto (mese target) e in CREATED: salvo solo la trx
                        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - No move needed (same batch and CREATED). Saving trx only. trxId={}, batchId={}",
                                suspendedTrx.getId(), newBatch.getId());
                        return rewardTransactionRepository.save(suspendedTrx)
                                .doOnSuccess(t -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Transaction saved without moving batch. trxId={}, rewardBatchId={}",
                                        t.getId(), t.getRewardBatchId()))
                                .then();
                    }

                    log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Moving counters. oldBatchId={}, newBatchId={}, oldBatchStatus={}, accruedRewardCents={}",
                            oldBatchId, newBatch.getId(), oldBatch.getStatus(), accruedRewardCents);

                    Mono<Void> moveCounters =
                            computeMoveCounters(oldBatch, oldBatchId, newBatch.getId(), accruedRewardCents);

                    return moveCounters
                            .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Counters moved successfully. oldBatchId={}, newBatchId={}, accruedRewardCents={}",
                                    oldBatchId, newBatch.getId(), accruedRewardCents))
                            .then(Mono.fromRunnable(() -> {
                                log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Applying batch move to transaction. trxId={}, fromBatchId={}, toBatchId={}, fromMonth={}, toMonth={}",
                                        suspendedTrx.getId(), oldBatchId, newBatch.getId(), oldBatch.getMonth(), newBatch.getMonth());
                                applyBatchMoveToTransaction(suspendedTrx, oldBatch, newBatch);
                            }))
                            .then(rewardTransactionRepository.save(suspendedTrx)
                                    .doOnSuccess(t -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Transaction saved after batch move. trxId={}, newRewardBatchId={}",
                                            t.getId(), t.getRewardBatchId())))
                            .then();
                })
                .doOnError(e -> log.error("[POINT-OF-SALE-TRANSACTION-SERVICE] - [MOVE-TO-CURRENT-MONTH-BATCH-AND-UPDATE-COUNTERS] - Failed. trxId={}, oldBatchId={}, error={}",
                        suspendedTrx != null ? suspendedTrx.getId() : null, oldBatchId, e.toString(), e));
    }

    private Mono<Void> computeMoveCounters(RewardBatch oldBatch,
                                           String oldBatchId,
                                           String newBatchId,
                                           long accruedRewardCents) {

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [COMPUTE-MOVE-COUNTERS] - Start. oldBatchId={}, newBatchId={}, oldBatchStatus={}, accruedRewardCents={}",
                oldBatchId, newBatchId, oldBatch != null ? oldBatch.getStatus() : null, accruedRewardCents);

        if (CREATED.equals(oldBatch.getStatus())) {
            log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [COMPUTE-MOVE-COUNTERS] - Old batch is CREATED: decrement old total and increment new total. oldBatchId={}, newBatchId={}, amount={}",
                    oldBatchId, newBatchId, accruedRewardCents);

            return rewardBatchService.decrementTotalAmountCents(oldBatchId, accruedRewardCents)
                    .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [COMPUTE-MOVE-COUNTERS] - Decrement old batch total done. oldBatchId={}, amount={}",
                            oldBatchId, accruedRewardCents))
                    .then(rewardBatchService.incrementTotalAmountCents(newBatchId, accruedRewardCents)
                            .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [COMPUTE-MOVE-COUNTERS] - Increment new batch total done. newBatchId={}, amount={}",
                                    newBatchId, accruedRewardCents)))
                    .then();
        }

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [COMPUTE-MOVE-COUNTERS] - Old batch is not CREATED: moving suspended counters to new batch. oldBatchId={}, newBatchId={}, amount={}",
                oldBatchId, newBatchId, accruedRewardCents);

        return rewardBatchService.moveSuspendToNewBatch(oldBatchId, newBatchId, accruedRewardCents)
                .doOnSuccess(v -> log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [COMPUTE-MOVE-COUNTERS] - moveSuspendToNewBatch done. oldBatchId={}, newBatchId={}, amount={}",
                        oldBatchId, newBatchId, accruedRewardCents))
                .then();
    }

    private void applyBatchMoveToTransaction(RewardTransaction suspendedTrx,
                                             RewardBatch oldBatch,
                                             RewardBatch newBatch) {

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [APPLY-BATCH-MOVE-TO-TRANSACTION] - Start. trxId={}, fromBatchId={}, toBatchId={}, oldBatchStatus={}, fromMonth={}, toMonth={}",
                suspendedTrx != null ? suspendedTrx.getId() : null,
                oldBatch != null ? oldBatch.getId() : null,
                newBatch != null ? newBatch.getId() : null,
                oldBatch != null ? oldBatch.getStatus() : null,
                oldBatch != null ? oldBatch.getMonth() : null,
                newBatch != null ? newBatch.getMonth() : null);

        suspendedTrx.setRewardBatchId(newBatch.getId());
        suspendedTrx.setUpdateDate(LocalDateTime.now());

        if (!CREATED.equals(oldBatch.getStatus())) {
            log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [APPLY-BATCH-MOVE-TO-TRANSACTION] - Updating rewardBatchLastMonthElaborated due to batch move. trxId={}, oldMonth={}, newMonth={}",
                    suspendedTrx.getId(), oldBatch.getMonth(), newBatch.getMonth());

            updateLastMonthElaboratedOnBatchMove(
                    suspendedTrx,
                    oldBatch.getMonth(),
                    newBatch.getMonth()
            );
        } else {
            log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [APPLY-BATCH-MOVE-TO-TRANSACTION] - Old batch is CREATED, rewardBatchLastMonthElaborated not updated. trxId={}",
                    suspendedTrx.getId());
        }

        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [APPLY-BATCH-MOVE-TO-TRANSACTION] - Completed. trxId={}, newRewardBatchId={}, rewardBatchLastMonthElaborated={}",
                suspendedTrx.getId(),
                suspendedTrx.getRewardBatchId(),
                suspendedTrx.getRewardBatchLastMonthElaborated());
    }

    private YearMonth getYearMonth(String yearMonthString) {
        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [GET-YEAR-MONTH] - Parsing yearMonth string. value={}", yearMonthString);

        YearMonth parsed = YearMonth.parse(yearMonthString.toLowerCase(), BATCH_MONTH_FORMAT);

        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [GET-YEAR-MONTH] - Parsed yearMonth. value={}, parsed={}", yearMonthString, parsed);

        return parsed;
    }

    private void updateLastMonthElaboratedOnBatchMove(RewardTransaction trx, String oldBatchMonth, String newBatchMonth) {
        log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-LAST-MONTH-ELABORATED-ON-BATCH-MOVE] - Start. trxId={}, currentLastMonthElaborated={}, oldBatchMonth={}, newBatchMonth={}",
                trx != null ? trx.getId() : null,
                trx != null ? trx.getRewardBatchLastMonthElaborated() : null,
                oldBatchMonth,
                newBatchMonth);

        if (trx.getRewardBatchLastMonthElaborated() == null
                || getYearMonth(trx.getRewardBatchLastMonthElaborated()).isBefore(getYearMonth(newBatchMonth))) {

            log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-LAST-MONTH-ELABORATED-ON-BATCH-MOVE] - Updating lastMonthElaborated. trxId={}, newValue={}",
                    trx.getId(), oldBatchMonth);

            trx.setRewardBatchLastMonthElaborated(oldBatchMonth);
        } else {
            log.info("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-LAST-MONTH-ELABORATED-ON-BATCH-MOVE] - No update needed. trxId={}, currentLastMonthElaborated={}, newBatchMonth={}",
                    trx.getId(), trx.getRewardBatchLastMonthElaborated(), newBatchMonth);
        }

        log.debug("[POINT-OF-SALE-TRANSACTION-SERVICE] - [UPDATE-LAST-MONTH-ELABORATED-ON-BATCH-MOVE] - Completed. trxId={}, rewardBatchLastMonthElaborated={}",
                trx.getId(), trx.getRewardBatchLastMonthElaborated());
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
                                                            ? rewardBatchRepository.decrementTotalAmountCents(oldRewardBatchId, accruedRewardCents).then()
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
