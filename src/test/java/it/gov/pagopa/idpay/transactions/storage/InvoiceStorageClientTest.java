package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InvoiceStorageClientTest {

    @Mock
    private BlobAsyncClient blobClientMock;
    @Mock
    private BlobServiceAsyncClient blobServiceAsyncClient;
    @Mock
    private BlobContainerAsyncClient blobContainerClient;
    @Mock
    private BlobStorageProperties propertiesMock;
    private InvoiceStorageClient invoiceStorageClient;

    @BeforeEach
    void init() {
        when(propertiesMock.getInvoiceTokenDurationSeconds()).thenReturn(60);
        lenient().doReturn(blobClientMock).when(blobContainerClient).getBlobAsyncClient(anyString());
        lenient().when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.just(userDelegationKey()));

        invoiceStorageClient = new InvoiceStorageClient(
            blobServiceAsyncClient,
            blobContainerClient,
            propertiesMock
        );
    }


    @Test
    void getFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenReturn("token");
        StepVerifier.create(invoiceStorageClient.getFileSignedUrl("fileId"))
                .expectNext("http://localhost:8080?token")
                .verifyComplete();

        verify(blobServiceAsyncClient).getUserDelegationKey(isNull(), any(OffsetDateTime.class));
        verify(blobContainerClient).getBlobAsyncClient("fileId");
        verify(blobClientMock).getBlobUrl();
        verify(blobClientMock).generateUserDelegationSas(any(), any());
    }

    @Test
    void uploadShouldReturnOK() {
        InputStream inputStream = new ByteArrayInputStream("test content".getBytes());
        String destination = "test/path/file.pdf";
        String contentType = "application/pdf";

        Response<BlockBlobItem> mockResponse = mock(Response.class);
        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class)))
            .thenReturn(Mono.just(mockResponse));

        StepVerifier.create(invoiceStorageClient.upload(inputStream, destination, contentType))
            .expectNext(mockResponse)
            .verifyComplete();

        verify(blobContainerClient).getBlobAsyncClient(destination);
        verify(blobClientMock).uploadWithResponse(any(BlobParallelUploadOptions.class));
    }

    @Test
    void uploadShouldPropagateAsyncError() {
        InputStream inputStream = new ByteArrayInputStream("test content".getBytes());
        String destination = "test/path/file.pdf";
        String contentType = "application/pdf";

        BlobStorageException uploadError = new BlobStorageException("Upload failed", null, null);
        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class)))
            .thenReturn(Mono.error(uploadError));

        StepVerifier.create(invoiceStorageClient.upload(inputStream, destination, contentType))
            .expectErrorMatches(error -> error == uploadError)
            .verify();
    }

    @Test
    void deleteFileShouldReturnOK() {
        String destination = "test/path/file.pdf";

        Response<Boolean> mockResponse = mock(Response.class);
        when(blobClientMock.deleteIfExistsWithResponse(
                eq(DeleteSnapshotsOptionType.INCLUDE), isNull(BlobRequestConditions.class)))
            .thenReturn(Mono.just(mockResponse));

        StepVerifier.create(invoiceStorageClient.deleteFile(destination))
            .expectNext(mockResponse)
            .verifyComplete();

        verify(blobContainerClient).getBlobAsyncClient(destination);
        verify(blobClientMock).deleteIfExistsWithResponse(
            eq(DeleteSnapshotsOptionType.INCLUDE), isNull(BlobRequestConditions.class));
    }

    @Test
    void deleteFileShouldReturnFalseWhenFileNotExists() {
        String destination = "test/path/nonexistent.pdf";

        Response<Boolean> mockResponse = mock(Response.class);
        when(mockResponse.getValue()).thenReturn(false);
        when(blobClientMock.deleteIfExistsWithResponse(
                eq(DeleteSnapshotsOptionType.INCLUDE), isNull(BlobRequestConditions.class)))
            .thenReturn(Mono.just(mockResponse));

        StepVerifier.create(invoiceStorageClient.deleteFile(destination))
            .expectNextMatches(response -> !response.getValue())
            .verifyComplete();
    }

    @Test
    void deleteFileShouldPropagateAsyncError() {
        String destination = "test/path/file.pdf";
        BlobStorageException deleteError = new BlobStorageException("Delete failed", null, null);
        when(blobClientMock.deleteIfExistsWithResponse(
                eq(DeleteSnapshotsOptionType.INCLUDE), isNull(BlobRequestConditions.class)))
            .thenReturn(Mono.error(deleteError));

        StepVerifier.create(invoiceStorageClient.deleteFile(destination))
            .expectErrorMatches(error -> error == deleteError)
            .verify();
    }

    @Test
    void getInvoiceFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenReturn("token");

        StepVerifier.create(invoiceStorageClient.getInvoiceFileSignedUrl("fileId"))
                .expectNext("http://localhost:8080?token")
                .verifyComplete();
    }

    @Test
    void getInvoiceFileSignedUrlShouldPropagateDelegationKeyError() {
        BlobStorageException delegationKeyError = new BlobStorageException("test", null, null);
        when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.error(delegationKeyError));

        StepVerifier.create(invoiceStorageClient.getInvoiceFileSignedUrl("fileId"))
                .expectErrorMatches(error -> error == delegationKeyError)
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
