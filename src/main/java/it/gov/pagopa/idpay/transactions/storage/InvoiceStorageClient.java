package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class InvoiceStorageClient extends AbstractBlobStorageClient {

    public InvoiceStorageClient(
            BlobServiceAsyncClient blobServiceAsyncClient,
            @Qualifier("invoiceContainerClient") BlobContainerAsyncClient blobContainerClient,
            BlobStorageProperties properties) {

        super(
                blobServiceAsyncClient,
                blobContainerClient,
                properties.getInvoiceTokenDurationSeconds()
        );
    }

    public Mono<String> getInvoiceFileSignedUrl(String blobPath) {
        return getFileSignedUrl(blobPath);
    }


    public Mono<Response<Boolean>> deleteFile(String destination) {
        log.info("Deleting file {} from azure blob container", Utilities.sanitizeString(destination));

        return containerClient.getBlobAsyncClient(destination)
                .deleteIfExistsWithResponse(DeleteSnapshotsOptionType.INCLUDE, null);
    }
}
