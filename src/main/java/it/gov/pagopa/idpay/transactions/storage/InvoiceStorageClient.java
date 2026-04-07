package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Clock;

@Component
@Slf4j
public class InvoiceStorageClient extends AbstractBlobStorageClient {

    public InvoiceStorageClient(
            BlobServiceClient blobServiceClient,
            @Qualifier("invoiceContainerClient") BlobContainerClient blobContainerClient,
            BlobStorageProperties properties,
            Clock clock) {

        super(
                blobServiceClient,
                blobContainerClient,
                properties.getInvoiceTokenDurationSeconds(),
                clock);
    }

    public String getInvoiceFileSignedUrl(String blobPath) {
        return getFileSignedUrl(blobPath);
    }


    public Response<Boolean> deleteFile(String destination) {
        log.info("Deleting file {} from azure blob container", Utilities.sanitizeString(destination));

        return containerClient.getBlobClient(destination)
                .deleteIfExistsWithResponse(DeleteSnapshotsOptionType.INCLUDE, null, null, null);
    }
}
