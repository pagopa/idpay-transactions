package it.gov.pagopa.idpay.transactions.storage;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_ON_GET_FILE_URL_REQUEST;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import it.gov.pagopa.common.web.exception.ClientException;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InvoiceStorageClient {

    private final BlobContainerClient blobContainerClient;
    private final BlobServiceClient blobServiceClient;
    private final Integer sasDurationSeconds;

    public InvoiceStorageClient(BlobServiceClient blobServiceClient,
        BlobContainerClient blobContainerClient,
        BlobStorageProperties properties) {
        this.blobServiceClient = blobServiceClient;
        this.blobContainerClient = blobContainerClient;
        this.sasDurationSeconds = properties.getInvoiceTokenDurationSeconds();
    }

    /**
     * Method to obtain a signedUrl for the file, given the blobPath, from the configured containerName,
     * and valid for the number of seconds provided as the sasDurationMinutes property
     * @param blobPath identifying the resource for which is required a signed url
     * @return url containing the generated token, if an error is encountered will throw ClientException
     */
    public String getFileSignedUrl(String blobPath) {

        // Create a SAS token that's valid for a limited amount of minutes
        OffsetDateTime expiryTime = OffsetDateTime.now().plusSeconds(sasDurationSeconds);

        UserDelegationKey userDelegationKey = blobServiceClient.getUserDelegationKey(
                null, // Immediate start for key validity
                OffsetDateTime.now().plusSeconds(sasDurationSeconds));

        // Assign read permissions to the SAS token
        BlobSasPermission sasPermission = new BlobSasPermission()
                .setReadPermission(true);

        BlobClient blobClient = blobContainerClient.getBlobClient(blobPath);
        BlobServiceSasSignatureValues sasSignatureValues = new BlobServiceSasSignatureValues
                (expiryTime, sasPermission);

        try {
            String sasToken = blobClient.generateUserDelegationSas(sasSignatureValues, userDelegationKey);
            return StringUtils.joinWith("?",
                    URLDecoder.decode(blobClient.getBlobUrl(), StandardCharsets.UTF_8), sasToken);
        } catch (BlobStorageException blobStorageException) {
            log.error("[InvoiceStorageClient] Encountered error on signature url recovery");
            throw new ClientException(
                    HttpStatus.INTERNAL_SERVER_ERROR, ERROR_ON_GET_FILE_URL_REQUEST, blobStorageException);
        }
    }

    public Response<BlockBlobItem> upload(InputStream inputStream, String destination, String contentType) {
        log.info("Uploading (contentType={}) into azure blob at destination {}", Utilities.sanitizeString(contentType), Utilities.sanitizeString(destination));

        return blobContainerClient.getBlobClient(destination)
                .uploadWithResponse(new BlobParallelUploadOptions(inputStream), null, null);
    }

    public Response<Boolean> deleteFile(String destination) {
        log.info("Deleting file {} from azure blob container", Utilities.sanitizeString(destination));

        return blobContainerClient.getBlobClient(destination)
                .deleteIfExistsWithResponse(DeleteSnapshotsOptionType.INCLUDE, null, null, null);
    }
}
