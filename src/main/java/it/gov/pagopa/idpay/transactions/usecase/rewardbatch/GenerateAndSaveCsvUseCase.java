package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.LongFunction;

import static it.gov.pagopa.idpay.transactions.usecase.rewardbatch.RewardBatchSharedUtils.REWARD_BATCHES_PATH_STORAGE_FORMAT;

@Service
@Slf4j
@AllArgsConstructor
public class GenerateAndSaveCsvUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;
    private final UserRestClient userRestClient;
    private final ApprovedRewardBatchBlobService approvedRewardBatchBlobService;

    private static final String CSV_HEADER = String.join(";",
            "Data e ora",
            "Elettrodomestico",
            "Codice Fiscale Beneficiario",
            "ID transazione",
            "Codice sconto",
            "Totale della spesa",
            "Sconto applicato",
            "Numero fattura",
            "Fattura",
            "Stato",
            "Punto vendita"
    );

    private static final String REWARD_BATCHES_REPORT_NAME_FORMAT = "%s_%s_%s.csv";

    public Mono<String> execute(String rewardBatchId, String initiativeId, String merchantId) {

        log.info("[GENERATE_AND_SAVE_CSV] Generate CSV for initiative {} and batch {}",
                Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(rewardBatchId));

        if (rewardBatchId.contains("..") || rewardBatchId.contains("/") || rewardBatchId.contains("\\")) {
            log.error("Invalid rewardBatchId for CSV filename: {}", Utilities.sanitizeString(rewardBatchId));
            return Mono.error(new IllegalArgumentException("Invalid batch id for CSV file generation"));
        }

        return rewardBatchRepository.findById(rewardBatchId)
                .flatMap(batch -> {

                    String pathPrefix = String.format(REWARD_BATCHES_PATH_STORAGE_FORMAT,
                            Utilities.sanitizeString(initiativeId),
                            Utilities.sanitizeString(batch.getMerchantId()),
                            Utilities.sanitizeString(rewardBatchId));

                    String reportFilename = String.format(REWARD_BATCHES_REPORT_NAME_FORMAT,
                            batch.getBusinessName(),
                            batch.getName(),
                            batch.getPosType().getDescription()).trim();

                    String filename = pathPrefix + reportFilename;

                    Flux<RewardTransaction> transactionFlux = rewardTransactionRepository.findByFilter(
                            rewardBatchId, initiativeId, List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED));

                    Flux<String> csvRowsFlux = transactionFlux
                            .concatMap(transaction -> {
                                if (transaction.getFiscalCode() == null || transaction.getFiscalCode().isEmpty()) {
                                    return userRestClient.retrieveUserInfo(transaction.getUserId())
                                            .map(cf -> {
                                                transaction.setFiscalCode(cf.getPii());
                                                return this.mapTransactionToCsvRow(transaction, initiativeId);
                                            });
                                } else {
                                    return Mono.just(this.mapTransactionToCsvRow(transaction, initiativeId));
                                }
                            });

                    Flux<String> fullCsvFlux = Flux.just(CSV_HEADER).concatWith(csvRowsFlux);

                    return fullCsvFlux
                            .collect(StringBuilder::new, (sb, s) -> sb.append(s).append("\n"))
                            .map(StringBuilder::toString)
                            .flatMap(csvContent -> this.uploadCsvToBlob(filename, csvContent))
                            .flatMap(uploadedPath -> {
                                batch.setFilename(reportFilename);
                                log.info("Updated batch {} with filename: {}", Utilities.sanitizeString(rewardBatchId), reportFilename);
                                return rewardBatchRepository.save(batch)
                                        .thenReturn(reportFilename);
                            });
                })
                .doOnTerminate(() -> log.info("CSV generation has been completed for batch: {}", Utilities.sanitizeString(rewardBatchId)));
    }

    String mapTransactionToCsvRow(RewardTransaction trx, String initiativeId) {

        Function<LocalDateTime, String> safeDateToString =
                date -> date != null
                        ? date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm"))
                        : "";

        LongFunction<String> centsToEuroString = cents -> {
            NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ITALY);
            numberFormat.setMinimumFractionDigits(2);
            numberFormat.setMaximumFractionDigits(2);
            return numberFormat.format(cents / 100.0);
        };

        String productName = trx.getAdditionalProperties().get("productName") != null
                ? trx.getAdditionalProperties().get("productName")
                : "";
        String productGtin = trx.getAdditionalProperties().get("productGtin") != null
                ? trx.getAdditionalProperties().get("productGtin")
                : "";

        String productInfo = productName + "\n" + productGtin;

        String invoiceNumber =
                trx.getInvoiceData() != null && trx.getInvoiceData().getDocNumber() != null
                        ? trx.getInvoiceData().getDocNumber()
                        : "";

        return String.join(";",
                safeDateToString.apply(trx.getTrxChargeDate()),
                csvField(productInfo),
                csvField(trx.getFiscalCode()),
                csvField(trx.getId()),
                csvField(trx.getTrxCode()),
                trx.getEffectiveAmountCents() != null
                        ? csvField(centsToEuroString.apply(trx.getEffectiveAmountCents()))
                        : "",
                trx.getRewards().get(initiativeId).getAccruedRewardCents() != null
                        ? csvField(centsToEuroString.apply(
                        trx.getRewards().get(initiativeId).getAccruedRewardCents()))
                        : "",
                csvField(invoiceNumber),
                csvField(trx.getInvoiceData().getFilename()),
                csvField(trx.getRewardBatchTrxStatus().getDescription()),
                csvField(trx.getFranchiseName())
        );
    }

    String csvField(String s) {
        if (s == null) {
            return "";
        }

        String escaped = s.replace("\"", "\"\"");

        boolean mustQuote =
                escaped.contains(";") ||
                        escaped.contains(",") ||
                        escaped.contains("\n") ||
                        escaped.contains("\r") ||
                        escaped.contains("\"");

        return mustQuote ? "\"" + escaped + "\"" : escaped;
    }

    public Mono<String> uploadCsvToBlob(String filename, String csvContent) {

        return Mono.fromCallable(() -> {
                    InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));
                    Response<BlockBlobItem> response = approvedRewardBatchBlobService.upload(
                            inputStream,
                            filename,
                            "text/csv; charset=UTF-8"
                    );

                    if (response.getStatusCode() != HttpStatus.CREATED.value()) {
                        log.error("Error uploading file to storage for file [{}]",
                                Utilities.sanitizeString(filename));
                        throw new ClientExceptionWithBody(HttpStatus.INTERNAL_SERVER_ERROR,
                                ExceptionConstants.ExceptionCode.GENERIC_ERROR,
                                "Error uploading csv file");
                    }
                    return filename;
                })
                .onErrorMap(BlobStorageException.class, e -> {
                    log.error("Azure Blob Storage upload failed for file {}", filename, e);
                    return new RuntimeException("Error uploading CSV to Blob Storage.", e);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}

