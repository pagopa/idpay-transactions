package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import it.gov.pagopa.common.web.exception.ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.OffsetDateTime;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ApprovedRewardBatchBlobServiceImplTest {

    @Mock
    private BlobAsyncClient blobClientMock;
    @Mock
    private BlobServiceAsyncClient blobServiceClient;
    @Mock
    private BlobContainerAsyncClient csvContainerClient;
    @Mock
    private BlobStorageProperties propertiesMock;
    private ApprovedRewardBatchBlobServiceImpl approvedService;

    @BeforeEach
    void init() {
        when(propertiesMock.getInvoiceTokenDurationSeconds()).thenReturn(60);
        lenient().doReturn(blobClientMock).when(csvContainerClient).getBlobAsyncClient(anyString());

        approvedService = new ApprovedRewardBatchBlobServiceImpl(
                blobServiceClient,
                csvContainerClient,
                propertiesMock
        );
    }

    @Test
    void getFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenReturn("token");
        when(blobServiceClient.getUserDelegationKey(isNull(), any(OffsetDateTime.class)))
                .thenReturn(Mono.just(userDelegationKey()));

        StepVerifier.create(approvedService.getFileSignedUrl("fileA.csv"))
                .expectNext("http://localhost:8080?token")
                .verifyComplete();

        verify(blobServiceClient).getUserDelegationKey(isNull(), any(OffsetDateTime.class));
        verify(csvContainerClient).getBlobAsyncClient("fileA.csv");
        verify(blobClientMock).getBlobUrl();
        verify(blobClientMock).generateUserDelegationSas(any(), any());
    }

    @Test
    void getFileSignedUrlShouldPropagateAsyncDelegationKeyError() {
        BlobStorageException delegationKeyError =
                new BlobStorageException("delegation key error", null, null);
        when(blobServiceClient.getUserDelegationKey(isNull(), any(OffsetDateTime.class)))
                .thenReturn(Mono.error(delegationKeyError));

        StepVerifier.create(approvedService.getFileSignedUrl("fileA.csv"))
                .expectErrorMatches(error -> error == delegationKeyError)
                .verify();

        verify(blobClientMock, never()).generateUserDelegationSas(any(), any());
    }

    @Test
    void getFileSignedUrlShouldMapSasGenerationError() {
        when(blobServiceClient.getUserDelegationKey(isNull(), any(OffsetDateTime.class)))
                .thenReturn(Mono.just(userDelegationKey()));
        when(blobClientMock.generateUserDelegationSas(any(), any()))
                .thenThrow(new BlobStorageException("sas error", null, null));

        StepVerifier.create(approvedService.getFileSignedUrl("fileA.csv"))
                .expectError(ClientException.class)
                .verify();
    }

    @Test
    void uploadShouldReturnOK() {
        InputStream input = new ByteArrayInputStream("csv content".getBytes());
        String destination = "path/fileA.csv";

        Response<BlockBlobItem> mockResponse = mock(Response.class);

        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class)))
                .thenReturn(Mono.just(mockResponse));

        StepVerifier.create(approvedService.upload(input, destination, "text/csv"))
                .expectNext(mockResponse)
                .verifyComplete();

        verify(csvContainerClient).getBlobAsyncClient(destination);
        verify(blobClientMock).uploadWithResponse(any(BlobParallelUploadOptions.class));
    }

    @Test
    void uploadShouldPropagateAsyncError() {
        InputStream input = new ByteArrayInputStream("csv content".getBytes());
        String destination = "path/fileA.csv";

        RuntimeException uploadError = new RuntimeException("upload error");
        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class)))
                .thenReturn(Mono.error(uploadError));

        StepVerifier.create(approvedService.upload(input, destination, "text/csv"))
                .expectErrorMatches(error -> error == uploadError)
                .verify();
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
