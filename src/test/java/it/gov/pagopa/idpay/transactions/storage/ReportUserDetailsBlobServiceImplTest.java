package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.UserDelegationKey;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportUserDetailsBlobServiceImplTest {

    @Test
    void getFileSignedUrlShouldUseUserDetailsContainer() {
        BlobServiceAsyncClient blobServiceAsyncClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient userDetailsContainer = mock(BlobContainerAsyncClient.class);
        BlobStorageProperties properties = mock(BlobStorageProperties.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        when(properties.getInvoiceTokenDurationSeconds()).thenReturn(60);
        when(userDetailsContainer.getBlobAsyncClient(anyString())).thenReturn(blobClient);
        when(blobClient.getBlobUrl()).thenReturn("https://storage.example/user-details.csv");
        when(blobClient.generateUserDelegationSas(any(), any())).thenReturn("signature");
        when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.just(userDelegationKey()));

        ReportUserDetailsBlobServiceImpl service = new ReportUserDetailsBlobServiceImpl(
                blobServiceAsyncClient, userDetailsContainer, properties
        );

        StepVerifier.create(service.getFileSignedUrl("initiative/report/user-details.csv"))
                .expectNext("https://storage.example/user-details.csv?signature")
                .verifyComplete();
        verify(userDetailsContainer).getBlobAsyncClient("initiative/report/user-details.csv");
    }

    private UserDelegationKey userDelegationKey() {
        OffsetDateTime now = OffsetDateTime.now();
        return new UserDelegationKey()
                .setSignedObjectId("object-id")
                .setSignedTenantId("tenant-id")
                .setSignedStart(now.minusMinutes(1))
                .setSignedExpiry(now.plusDays(1))
                .setSignedService("b")
                .setSignedVersion("2023-11-03")
                .setValue("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    }
}
