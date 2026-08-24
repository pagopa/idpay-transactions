package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReportUserDetailsBlobServiceImpl extends AbstractBlobStorageClient implements ReportBlobService {

    public ReportUserDetailsBlobServiceImpl(
            BlobServiceClient blobServiceClient,
            BlobServiceAsyncClient blobServiceAsyncClient,
            @Qualifier("reportsUserDetailsContainerClient") BlobContainerClient reportsContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceClient, blobServiceAsyncClient, reportsContainerClient, properties.getInvoiceTokenDurationSeconds());
    }
}
