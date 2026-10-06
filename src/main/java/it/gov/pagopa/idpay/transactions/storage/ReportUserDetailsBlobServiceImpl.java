package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ReportUserDetailsBlobServiceImpl extends AbstractBlobStorageClient implements ReportBlobService {

    public ReportUserDetailsBlobServiceImpl(
            BlobServiceAsyncClient blobServiceAsyncClient,
            @Qualifier("reportsUserDetailsContainerClient") BlobContainerAsyncClient reportsContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceAsyncClient, reportsContainerClient, properties.getInvoiceTokenDurationSeconds());
    }
}
