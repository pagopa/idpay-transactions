package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import it.gov.pagopa.common.web.exception.ClientException;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_ON_GET_FILE_URL_REQUEST;

@Component
@Slf4j
public class CsvStorageClient {

    private final BlobContainerClient csvContainerClient;
    private final BlobServiceClient blobServiceClient;
    private final Integer sasDurationSeconds;

    public CsvStorageClient(
            BlobServiceClient blobServiceClient,
            @Qualifier("csvContainerClient") BlobContainerClient csvContainerClient,
            BlobStorageProperties properties) {

        this.blobServiceClient = blobServiceClient;
        this.csvContainerClient = csvContainerClient;
        this.sasDurationSeconds = properties.getInvoiceTokenDurationSeconds();
    }

    public String getCsvFileSignedUrl(String blobPath) {

        OffsetDateTime expiryTime = OffsetDateTime.now().plusSeconds(sasDurationSeconds);

        UserDelegationKey userDelegationKey = blobServiceClient.getUserDelegationKey(
                null,
                OffsetDateTime.now().plusSeconds(sasDurationSeconds));

        BlobSasPermission sasPermission = new BlobSasPermission().setReadPermission(true);

        BlobClient blobClient = csvContainerClient.getBlobClient(blobPath);

        BlobServiceSasSignatureValues sasSignatureValues =
                new BlobServiceSasSignatureValues(expiryTime, sasPermission);

        try {
            String sasToken = blobClient.generateUserDelegationSas(sasSignatureValues, userDelegationKey);
            return StringUtils.joinWith("?",
                    URLDecoder.decode(blobClient.getBlobUrl(), StandardCharsets.UTF_8), sasToken);
        } catch (BlobStorageException e) {
            log.error("[CsvStorageClient] Encountered error on signature url recovery");
            throw new ClientException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ERROR_ON_GET_FILE_URL_REQUEST,
                    e
            );
        }
    }

    public Response<BlockBlobItem> upload(InputStream inputStream, String destination, String contentType) {
        log.info("Uploading (contentType={}) into azure blob at destination {}", Utilities.sanitizeString(contentType), Utilities.sanitizeString(destination));

        return csvContainerClient.getBlobClient(destination)
                .uploadWithResponse(new BlobParallelUploadOptions(inputStream), null, null);
    }
}
