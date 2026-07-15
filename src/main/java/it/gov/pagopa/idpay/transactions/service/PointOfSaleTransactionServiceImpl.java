package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionKafkaDTO;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionKafkaMapper;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.notifier.TransactionNotifierService;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicy;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.Objects;

import static it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus.CREATED;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.GENERIC_ERROR;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE;


@Service
@Slf4j
public class PointOfSaleTransactionServiceImpl implements PointOfSaleTransactionService {

    private final RewardTransactionRepository rewardTransactionRepository;
    private final InvoiceStorageClient invoiceStorageClient;
    private final RewardBatchService rewardBatchService;
    private final RewardBatchRepository rewardBatchRepository;
    private final TransactionErrorNotifierService transactionErrorNotifierService;
    private final TransactionNotifierService transactionNotifierService;

    protected PointOfSaleTransactionServiceImpl(
            RewardTransactionRepository rewardTransactionRepository,
            InvoiceStorageClient invoiceStorageClient,
            RewardBatchService rewardBatchService,
            RewardBatchRepository rewardBatchRepository,
            TransactionErrorNotifierService transactionErrorNotifierService,
            TransactionNotifierService transactionNotifierService) {
        this.rewardTransactionRepository = rewardTransactionRepository;
        this.invoiceStorageClient = invoiceStorageClient;
        this.rewardBatchService = rewardBatchService;
        this.rewardBatchRepository = rewardBatchRepository;
        this.transactionErrorNotifierService = transactionErrorNotifierService;
        this.transactionNotifierService = transactionNotifierService;
    }


    public Mono<Void> updateInvoiceTransaction(String transactionId, String merchantId,
                                               FilePart file, String docNumber, InvoiceLifecyclePolicy policy) {

        log.info("[UPDATE_INVOICE_FILE_SERVICE] - [updateInvoiceTransaction] - start | trxId={} merchantId={} docNumber={} filename={}",
                Utilities.sanitizeString(transactionId), Utilities.sanitizeString(merchantId), Utilities.sanitizeString(docNumber), file != null ? Utilities.sanitizeString(file.filename()) : null);

        Utilities.checkFileExtensionOrThrow(file);

        return rewardTransactionRepository
                .findTransaction(merchantId, transactionId)
                .switchIfEmpty(Mono.defer(() -> Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, TRANSACTION_MISSING_INVOICE))))
                .flatMap(trx -> validateBatchAndUpdateInvoiceFlow(trx, file, docNumber, policy));
    }

    private Mono<Void> validateBatchAndUpdateInvoiceFlow(RewardTransaction trx,
                                                         FilePart file,
                                                         String docNumber,
                                                         InvoiceLifecyclePolicy policy) {

        String oldBatchId = requireRewardBatchId(trx);

        return rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(trx.getMerchantId(), trx.getInitiatives().getFirst(), oldBatchId)
                .switchIfEmpty(Mono.defer(() -> Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND))))
                .flatMap(oldBatch ->

                        policy.validate(trx, oldBatch)
                                .flatMap(t-> updateInvoiceFileAndFields(trx, file, docNumber))
                                .flatMap(savedTrx -> suspendAndMoveTransaction(savedTrx, oldBatch))
                                .then()
                );
    }

    private String requireRewardBatchId(RewardTransaction trx) {
        String oldBatchId = trx.getRewardBatchId();
        if (oldBatchId == null) {
            throw new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_NOT_FOUND);
        }

        return oldBatchId;
    }

    private Mono<RewardTransaction> updateInvoiceFileAndFields(RewardTransaction trx,
                                                               FilePart file,
                                                               String docNumber) {

        InvoiceData oldDocumentData = trx.getInvoiceData();

        return replaceInvoiceFile(file, oldDocumentData, trx.getMerchantId(), trx.getPointOfSaleId(), trx.getId())
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

        return rewardBatchService.findOrCreateBatch(oldBatch.getInitiativeId(), oldBatch.getMerchantId(), posType, targetMonth.toString(), businessName);
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
        boolean wasToCheckOrConsultable = (oldTransaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.CONSULTABLE
                || oldTransaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.TO_CHECK);
        boolean isNotInvoiced = !SyncTrxStatus.INVOICED.name().equals(oldTransaction.getStatus());

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
                    .decrementTrxElaborated(wasRejected ? 1L : 0L)
                    .decrementApprovedAmountCents(wasToCheckOrConsultable && isNotInvoiced ? accruedRewardCents : 0L);

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
                            .then(rewardBatchRepository.updateTotals(oldBatch.getInitiativeId(), oldBatch.getId(), oldBatchCounter))
                            .then(rewardBatchRepository.updateTotals(newBatch.getInitiativeId(), newBatch.getId(), newBatchCounter))
                            .thenReturn(oldTransaction);
                });
    }

    @Override
    public Mono<Void> reversalTransaction(
            String transactionId,
            String merchantId,
            FilePart file,
            String docNumber,
            InvoiceLifecyclePolicy policy
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
                .flatMap(rt -> validateTransactionAgainstInvoiceLifecyclePolicy(rt, policy))
                .flatMap(rt -> {
                    final String oldRewardBatchId = rt.getRewardBatchId();
                    final RewardBatchTrxStatus oldBatchTrxStatus = rt.getRewardBatchTrxStatus();
                    final boolean wasSuspended = RewardBatchTrxStatus.SUSPENDED.equals(oldBatchTrxStatus);
                    final boolean wasRejected = RewardBatchTrxStatus.REJECTED.equals(oldBatchTrxStatus);
                    final boolean wasToCheckOrConsultable = RewardBatchTrxStatus.CONSULTABLE.equals(oldBatchTrxStatus)
                            || RewardBatchTrxStatus.TO_CHECK.equals(oldBatchTrxStatus);
                    final boolean isNotInvoiced = !SyncTrxStatus.INVOICED.name().equals(rt.getStatus());


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
                    if(wasToCheckOrConsultable && isNotInvoiced){
                        counters.decrementApprovedAmountCents(accruedRewardCents);
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
                                                    ? rewardBatchRepository.updateTotals(initiativeId, oldRewardBatchId, counters).then()
                                                    : Mono.empty();


                                    Mono<Void> sendToQueueMono = sendReversedInvoicedTransactionNotification(RewardTransactionKafkaMapper.toDto(rt));

                                    return saveTransactionMono
                                            .then(updateBatchTotalsMono)
                                            .then(sendToQueueMono);
                                }));
                    });
                })
                .doOnError(e -> log.error("[REVERSAL-TRANSACTION-SERVICE] Error during reversalTransaction [transactionId={}, merchantId={}, error={}]",
                        sanitizedTransactionId, sanitizedMerchantId, e.getMessage()))
                .then();
    }

    private Mono<RewardTransaction> validateTransactionAgainstInvoiceLifecyclePolicy(
            RewardTransaction rt,
            InvoiceLifecyclePolicy policy) {

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
                .flatMap(batch -> policy.validate(rt, batch));
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


    @Override
    public Mono<List<FranchisePointOfSaleDTO>> getDistinctFranchiseAndPosByRewardBatchId(String rewardBatchId, String merchantId) {
        log.info("[POINT_OF_SALE_TRANSACTION_SERVICE] - Get point of sale for reward batch id [{}]", Utilities.sanitizeString(rewardBatchId));
        return rewardTransactionRepository
                .findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId, merchantId)
                .collectList();
    }

}