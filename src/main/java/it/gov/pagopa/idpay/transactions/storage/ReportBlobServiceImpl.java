package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReportBlobServiceImpl extends AbstractBlobStorageClient implements ReportBlobService {

    public ReportBlobServiceImpl(
            BlobServiceClient blobServiceClient,
            @Qualifier("reportsContainerClient") BlobContainerClient reportsContainerClient,
            BlobStorageProperties properties) {

        super(blobServiceClient, reportsContainerClient, properties.getInvoiceTokenDurationSeconds());
    }
}
