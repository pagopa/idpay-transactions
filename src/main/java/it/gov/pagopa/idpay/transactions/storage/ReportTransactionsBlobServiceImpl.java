package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Clock;

@Component
@Slf4j
public class ReportTransactionsBlobServiceImpl extends AbstractBlobStorageClient implements ReportBlobService {

    public ReportTransactionsBlobServiceImpl(
            BlobServiceClient blobServiceClient,
            @Qualifier("reportsTransactionsContainerClient") BlobContainerClient reportsContainerClient,
            BlobStorageProperties properties,
            Clock clock) {

        super(blobServiceClient, reportsContainerClient, properties.getInvoiceTokenDurationSeconds(), clock);
    }
}
