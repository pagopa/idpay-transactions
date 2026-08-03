package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportUserDetailsBlobServiceImplTest {

    @Test
    void getFileSignedUrlShouldUseUserDetailsContainer() {
        BlobServiceClient blobServiceClient = mock(BlobServiceClient.class);
        BlobContainerClient userDetailsContainer = mock(BlobContainerClient.class);
        BlobStorageProperties properties = mock(BlobStorageProperties.class);
        BlobClient blobClient = mock(BlobClient.class);
        when(properties.getInvoiceTokenDurationSeconds()).thenReturn(60);
        when(userDetailsContainer.getBlobClient(anyString())).thenReturn(blobClient);
        when(blobClient.getBlobUrl()).thenReturn("https://storage.example/user-details.csv");
        when(blobClient.generateUserDelegationSas(any(), any())).thenReturn("signature");

        ReportUserDetailsBlobServiceImpl service = new ReportUserDetailsBlobServiceImpl(
                blobServiceClient, userDetailsContainer, properties
        );

        assertEquals(
                "https://storage.example/user-details.csv?signature",
                service.getFileSignedUrl("initiative/report/user-details.csv")
        );
        verify(userDetailsContainer).getBlobClient("initiative/report/user-details.csv");
    }
}
