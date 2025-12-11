package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Component
@Slf4j
public class ApprovedRewardBatchBlobServiceImpl extends AbstractBlobStorageClient implements ApprovedRewardBatchBlobService {

    public ApprovedRewardBatchBlobServiceImpl(
            BlobServiceClient blobServiceClient,
            @Qualifier("csvContainerClient") BlobContainerClient csvContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceClient, csvContainerClient, properties.getInvoiceTokenDurationSeconds());
    }

    @Override
    public Response<BlockBlobItem> upload(InputStream inputStream, String destination, String contentType) {
        log.info("[ApprovedRewardBatch] Uploading file to {}", Utilities.sanitizeString(destination));

        return containerClient.getBlobClient(destination)
                .uploadWithResponse(new BlobParallelUploadOptions(inputStream), null, null);
    }
}
