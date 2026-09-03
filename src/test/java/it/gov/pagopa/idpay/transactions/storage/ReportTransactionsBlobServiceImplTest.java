package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReportTransactionsBlobServiceImplTest {

    @Mock
    private BlobAsyncClient blobClientMock;
    @Mock
    private BlobServiceAsyncClient blobServiceAsyncClient;

    @Mock
    private BlobContainerAsyncClient reportsContainerClient;

    @Mock
    private BlobStorageProperties propertiesMock;
    private ReportTransactionsBlobServiceImpl reportService;

    @BeforeEach
    void init() {
        when(propertiesMock.getInvoiceTokenDurationSeconds()).thenReturn(60);
        lenient().when(reportsContainerClient.getBlobAsyncClient(anyString()))
                .thenReturn(blobClientMock);
        lenient().when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.just(userDelegationKey()));

        reportService = new ReportTransactionsBlobServiceImpl(
                blobServiceAsyncClient,
                reportsContainerClient,
                propertiesMock
        );
    }

    @Test
    void getFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenReturn("token");

        StepVerifier.create(reportService.getFileSignedUrl("fileA.csv"))
                .expectNext("http://localhost:8080?token")
                .verifyComplete();
    }

    @Test
    void getFileSignedUrlShouldPropagateDelegationKeyError() {
        BlobStorageException delegationKeyError = new BlobStorageException("delegation key error", null, null);
        when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.error(delegationKeyError));

        StepVerifier.create(reportService.getFileSignedUrl("fileA.csv"))
                .expectErrorMatches(error -> error == delegationKeyError)
                .verify();
    }

    @Test
    void uploadShouldReturnOK() {
        InputStream input = new ByteArrayInputStream("report content".getBytes());
        String destination = "path/report.csv";

        Response<BlockBlobItem> mockResponse = mock(Response.class);

        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class)))
                .thenReturn(Mono.just(mockResponse));

        StepVerifier.create(reportService.upload(input, destination, "text/csv"))
                .expectNext(mockResponse)
                .verifyComplete();

        verify(reportsContainerClient).getBlobAsyncClient(destination);
        verify(blobClientMock).uploadWithResponse(any(BlobParallelUploadOptions.class));
    }

    @Test
    void uploadShouldPropagateAsyncError() {
        InputStream input = new ByteArrayInputStream("report content".getBytes());
        String destination = "path/report.csv";

        RuntimeException uploadError = new RuntimeException("upload error");
        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class)))
                .thenReturn(Mono.error(uploadError));

        StepVerifier.create(reportService.upload(input, destination, "text/csv"))
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
