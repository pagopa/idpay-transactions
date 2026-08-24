package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ApprovedRewardBatchBlobServiceImpl extends AbstractBlobStorageClient implements ApprovedRewardBatchBlobService {

    public ApprovedRewardBatchBlobServiceImpl(
            BlobServiceAsyncClient blobServiceAsyncClient,
            @Qualifier("rewardBatchesContainerClient") BlobContainerAsyncClient csvContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceAsyncClient, csvContainerClient, properties.getInvoiceTokenDurationSeconds());
    }

}
