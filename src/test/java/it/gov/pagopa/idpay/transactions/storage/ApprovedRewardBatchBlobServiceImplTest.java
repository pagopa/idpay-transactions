package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import it.gov.pagopa.common.web.exception.ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ApprovedRewardBatchBlobServiceImplTest {

    @Mock
    private BlobClient blobClientMock;
    @Mock
    private BlobServiceClient blobServiceClient;
    @Mock
    private BlobContainerClient csvContainerClient;
    @Mock
    private BlobStorageProperties propertiesMock;

    private ApprovedRewardBatchBlobServiceImpl approvedService;

    @BeforeEach
    void init() {
        when(propertiesMock.getInvoiceTokenDurationSeconds()).thenReturn(60);
        lenient().doReturn(blobClientMock).when(csvContainerClient).getBlobClient(anyString());

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

        String url = approvedService.getFileSignedUrl("fileA.csv");

        assertNotNull(url);
        assertEquals("http://localhost:8080?token", url);
    }

    @Test
    void getFileSignedUrlShouldThrowException() {
        when(blobClientMock.generateUserDelegationSas(any(), any()))
                .thenThrow(new BlobStorageException("sas error", null, null));

        assertThrows(ClientException.class,
                () -> approvedService.getFileSignedUrl("fileA.csv"));
    }

    @Test
    void uploadShouldReturnOK() {
        InputStream input = new ByteArrayInputStream("csv content".getBytes());
        String destination = "path/fileA.csv";

        Response<BlockBlobItem> mockResponse = mock(Response.class);

        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
                .thenReturn(mockResponse);

        Response<BlockBlobItem> result =
                approvedService.upload(input, destination, "text/csv");

        assertNotNull(result);
        verify(csvContainerClient).getBlobClient(destination);
        verify(blobClientMock).uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any());
    }

    @Test
    void uploadShouldThrowException() {
        InputStream input = new ByteArrayInputStream("csv content".getBytes());
        String destination = "path/fileA.csv";

        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
                .thenThrow(new RuntimeException("upload error"));

        assertThrows(RuntimeException.class,
                () -> approvedService.upload(input, destination, "text/csv"));
    }
}
