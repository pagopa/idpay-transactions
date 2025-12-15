package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ApprovedRewardBatchBlobServiceImpl extends AbstractBlobStorageClient implements ApprovedRewardBatchBlobService {

    public ApprovedRewardBatchBlobServiceImpl(
            BlobServiceClient blobServiceClient,
            @Qualifier("rewardBatchesContainerClient") BlobContainerClient csvContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceClient, csvContainerClient, properties.getInvoiceTokenDurationSeconds());
    }

}
