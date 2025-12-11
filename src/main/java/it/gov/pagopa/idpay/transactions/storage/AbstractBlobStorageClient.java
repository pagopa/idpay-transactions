package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import it.gov.pagopa.common.web.exception.ClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_ON_GET_FILE_URL_REQUEST;

@Slf4j
public abstract class AbstractBlobStorageClient {

    protected final BlobServiceClient blobServiceClient;
    protected final BlobContainerClient containerClient;
    protected final Integer sasDurationSeconds;

    protected AbstractBlobStorageClient(
            BlobServiceClient blobServiceClient,
            BlobContainerClient containerClient,
            Integer sasDurationSeconds) {

        this.blobServiceClient = blobServiceClient;
        this.containerClient = containerClient;
        this.sasDurationSeconds = sasDurationSeconds;
    }

    public String getFileSignedUrl(String blobPath) {
        OffsetDateTime expiryTime = OffsetDateTime.now().plusSeconds(sasDurationSeconds);
        UserDelegationKey userDelegationKey =
                blobServiceClient.getUserDelegationKey(null, expiryTime);

        BlobSasPermission sasPermission = new BlobSasPermission().setReadPermission(true);
        BlobClient blobClient = containerClient.getBlobClient(blobPath);
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiryTime, sasPermission);

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
}
