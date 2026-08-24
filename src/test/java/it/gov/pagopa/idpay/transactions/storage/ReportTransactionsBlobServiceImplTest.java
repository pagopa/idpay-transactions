package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReportTransactionsBlobServiceImplTest {

    @Mock
    private BlobClient blobClientMock;

    @Mock
    private BlobServiceClient blobServiceClient;
    @Mock
    private BlobServiceAsyncClient blobServiceAsyncClient;

    @Mock
    private BlobContainerClient reportsContainerClient;

    @Mock
    private BlobStorageProperties propertiesMock;
    @Mock
    private UserDelegationKey userDelegationKey;

    private ReportTransactionsBlobServiceImpl reportService;

    @BeforeEach
    void init() {
        when(propertiesMock.getInvoiceTokenDurationSeconds()).thenReturn(60);

        lenient().when(reportsContainerClient.getBlobClient(anyString()))
                .thenReturn(blobClientMock);
        lenient().when(blobServiceAsyncClient.getUserDelegationKey(any(), any()))
                .thenReturn(Mono.just(userDelegationKey));

        reportService = new ReportTransactionsBlobServiceImpl(
                blobServiceClient,
                blobServiceAsyncClient,
                reportsContainerClient,
                propertiesMock
        );
    }

    @Test
    void getFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any()))
                .thenReturn("token");

        StepVerifier.create(reportService.getFileSignedUrl("fileA.csv"))
                .expectNext("http://localhost:8080?token")
                .verifyComplete();
    }

    @Test
    void getFileSignedUrlShouldThrowException() {
        when(blobClientMock.generateUserDelegationSas(any(), any()))
                .thenThrow(new BlobStorageException("sas error", null, null));

        StepVerifier.create(reportService.getFileSignedUrl("fileA.csv"))
                .expectError(ClientException.class)
                .verify();
    }

    @Test
    void uploadShouldReturnOK() {
        InputStream input = new ByteArrayInputStream("report content".getBytes());
        String destination = "path/report.csv";

        Response<BlockBlobItem> mockResponse = mock(Response.class);

        when(blobClientMock.uploadWithResponse(
                any(BlobParallelUploadOptions.class),
                any(),
                any()
        )).thenReturn(mockResponse);

        Response<BlockBlobItem> result =
                reportService.upload(input, destination, "text/csv");

        assertNotNull(result);

        verify(reportsContainerClient).getBlobClient(destination);
        verify(blobClientMock).uploadWithResponse(
                any(BlobParallelUploadOptions.class),
                any(),
                any()
        );
    }

    @Test
    void uploadShouldThrowException() {
        InputStream input = new ByteArrayInputStream("report content".getBytes());
        String destination = "path/report.csv";

        when(blobClientMock.uploadWithResponse(
                any(BlobParallelUploadOptions.class),
                any(),
                any()
        )).thenThrow(new RuntimeException("upload error"));

        assertThrows(RuntimeException.class,
                () -> reportService.upload(input, destination, "text/csv"));
    }
}
