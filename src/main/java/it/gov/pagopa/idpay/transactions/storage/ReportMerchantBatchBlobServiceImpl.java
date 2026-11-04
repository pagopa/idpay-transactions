package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ReportMerchantBatchBlobServiceImpl extends AbstractBlobStorageClient implements ReportBlobService {

    public ReportMerchantBatchBlobServiceImpl(
            BlobServiceAsyncClient blobServiceAsyncClient,
            @Qualifier("reportsMerchantBatchContainerClient") BlobContainerAsyncClient reportsContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceAsyncClient, reportsContainerClient, properties.getInvoiceTokenDurationSeconds());
    }
}

