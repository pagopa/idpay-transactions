package it.gov.pagopa.idpay.transactions.storage;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import it.gov.pagopa.common.web.exception.ClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_ON_GET_FILE_URL_REQUEST;

@Component
@Slf4j
public class InvoiceStorageClient {

    private final BlobContainerClient blobContainerClient;

    private final BlobServiceClient blobServiceClient;

    private final Integer sasDurationSeconds;

    @Autowired
    public InvoiceStorageClient(
            @Value("${spring.cloud.azure.storage.blob.invoice.storageAccountName}") String storageAccountName,
            @Value("${spring.cloud.azure.storage.blob.invoice.containerName}") String containerName,
            @Value("${spring.cloud.azure.storage.blob.invoice.invoiceTokenDurationSeconds}") Integer sasDurationSeconds) {

            DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();

            blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint("https://" + storageAccountName + ".blob.core.windows.net")
                    .credential(credential)
                    .buildClient();

            blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
            this.sasDurationSeconds = sasDurationSeconds;
    }

    public InvoiceStorageClient(
            BlobServiceClient blobServiceClient,
            BlobContainerClient blobContainerClient,
            Integer sasDurationSeconds) {
        this.blobServiceClient = blobServiceClient;
        this.blobContainerClient = blobContainerClient;
        this.sasDurationSeconds = sasDurationSeconds;
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
}
