package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
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
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_ON_GET_FILE_URL_REQUEST;

@Slf4j
public abstract class AbstractBlobStorageClient {

    protected final BlobServiceClient blobServiceClient;
    protected final BlobServiceAsyncClient blobServiceAsyncClient;
    protected final BlobContainerClient containerClient;
    protected final Integer sasDurationSeconds;

    protected AbstractBlobStorageClient(
            BlobServiceClient blobServiceClient,
            BlobServiceAsyncClient blobServiceAsyncClient,
            BlobContainerClient containerClient,
            Integer sasDurationSeconds) {

        this.blobServiceClient = blobServiceClient;
        this.blobServiceAsyncClient = blobServiceAsyncClient;
        this.containerClient = containerClient;
        this.sasDurationSeconds = sasDurationSeconds;
    }

    public Mono<String> getFileSignedUrl(String blobPath) {
        OffsetDateTime expiryTime = OffsetDateTime.now().plusSeconds(sasDurationSeconds);
        BlobSasPermission sasPermission = new BlobSasPermission().setReadPermission(true);
        BlobClient blobClient = containerClient.getBlobClient(blobPath);
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiryTime, sasPermission);

        return blobServiceAsyncClient.getUserDelegationKey(null, expiryTime)
                .map(userDelegationKey -> generateSignedUrl(blobClient, sasValues, userDelegationKey));
    }

    private String generateSignedUrl(
            BlobClient blobClient,
            BlobServiceSasSignatureValues sasValues,
            UserDelegationKey userDelegationKey) {
        try {
            String sasToken = blobClient.generateUserDelegationSas(sasValues, userDelegationKey);
            return StringUtils.joinWith("?",
                    URLDecoder.decode(blobClient.getBlobUrl(), StandardCharsets.UTF_8),
                    sasToken);
        } catch (BlobStorageException e) {
            log.error("Error generating SAS token");
            throw new ClientException(HttpStatus.INTERNAL_SERVER_ERROR, ERROR_ON_GET_FILE_URL_REQUEST, e);
        }
    }

    public Response<BlockBlobItem> upload(InputStream inputStream, String destination, String contentType) {
        log.info("Uploading (contentType={}) into azure blob at destination {}",
                Utilities.sanitizeString(contentType),
                Utilities.sanitizeString(destination));

        return containerClient.getBlobClient(destination)
                .uploadWithResponse(new BlobParallelUploadOptions(inputStream), null, null);
    }
}
