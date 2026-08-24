package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.UserDelegationKey;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportUserDetailsBlobServiceImplTest {

    @Test
    void getFileSignedUrlShouldUseUserDetailsContainer() {
        BlobServiceClient blobServiceClient = mock(BlobServiceClient.class);
        BlobServiceAsyncClient blobServiceAsyncClient = mock(BlobServiceAsyncClient.class);
        BlobContainerClient userDetailsContainer = mock(BlobContainerClient.class);
        BlobStorageProperties properties = mock(BlobStorageProperties.class);
        BlobClient blobClient = mock(BlobClient.class);
        when(properties.getInvoiceTokenDurationSeconds()).thenReturn(60);
        when(userDetailsContainer.getBlobClient(anyString())).thenReturn(blobClient);
        when(blobClient.getBlobUrl()).thenReturn("https://storage.example/user-details.csv");
        when(blobClient.generateUserDelegationSas(any(), any())).thenReturn("signature");
        when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.just(mock(UserDelegationKey.class)));

        ReportUserDetailsBlobServiceImpl service = new ReportUserDetailsBlobServiceImpl(
                blobServiceClient, blobServiceAsyncClient, userDetailsContainer, properties
        );

        StepVerifier.create(service.getFileSignedUrl("initiative/report/user-details.csv"))
                .expectNext("https://storage.example/user-details.csv?signature")
                .verifyComplete();
        verify(userDetailsContainer).getBlobClient("initiative/report/user-details.csv");
    }
}
