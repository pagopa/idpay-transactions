package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InvoiceStorageClientTest {

    @Mock
    private BlobClient blobClientMock;
    @Mock
    private BlobServiceClient blobServiceClient;
    @Mock
    private BlobContainerClient blobContainerClient;
    @Mock
    private BlobStorageProperties propertiesMock;

    private InvoiceStorageClient invoiceStorageClient;

    @BeforeEach
    void init() {
        when(propertiesMock.getInvoiceTokenDurationSeconds()).thenReturn(60);
        lenient().doReturn(blobClientMock).when(blobContainerClient).getBlobClient(anyString());

        invoiceStorageClient = new InvoiceStorageClient(
            blobServiceClient,
            blobContainerClient,
            propertiesMock
        );
    }


    @Test
    void getFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenReturn("token");
        String url = invoiceStorageClient.getFileSignedUrl("fileId");
        assertNotNull(url);
        assertEquals("http://localhost:8080?token", url);
    }

    @Test
    void getFileSignedUrlShouldReturnKO() {
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenAnswer(item -> {
            throw new BlobStorageException("test", null, null);
        });
        assertThrows(ClientException.class, () -> invoiceStorageClient
                .getFileSignedUrl( "fileId"));
    }

    @Test
    void uploadShouldReturnOK() {
        InputStream inputStream = new ByteArrayInputStream("test content".getBytes());
        String destination = "test/path/file.pdf";
        String contentType = "application/pdf";

        Response<BlockBlobItem> mockResponse = mock(Response.class);
        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
            .thenReturn(mockResponse);

        Response<BlockBlobItem> result = invoiceStorageClient.upload(inputStream, destination, contentType);

        assertNotNull(result);
        verify(blobContainerClient).getBlobClient(destination);
        verify(blobClientMock).uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any());
    }

    @Test
    void uploadShouldThrowException() {
        InputStream inputStream = new ByteArrayInputStream("test content".getBytes());
        String destination = "test/path/file.pdf";
        String contentType = "application/pdf";

        when(blobClientMock.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
            .thenThrow(new BlobStorageException("Upload failed", null, null));

        assertThrows(BlobStorageException.class, () ->
            invoiceStorageClient.upload(inputStream, destination, contentType));
    }

    @Test
    void deleteFileShouldReturnOK() {
        String destination = "test/path/file.pdf";

        Response<Boolean> mockResponse = mock(Response.class);
        when(mockResponse.getValue()).thenReturn(true);
        when(blobClientMock.deleteIfExistsWithResponse(any(), any(), any(), any()))
            .thenReturn(mockResponse);

        Response<Boolean> result = invoiceStorageClient.deleteFile(destination);

        assertNotNull(result);
        assertTrue(result.getValue());
        verify(blobContainerClient).getBlobClient(destination);
        verify(blobClientMock).deleteIfExistsWithResponse(
            eq(DeleteSnapshotsOptionType.INCLUDE), any(), any(), any());
    }

    @Test
    void deleteFileShouldReturnFalseWhenFileNotExists() {
        String destination = "test/path/nonexistent.pdf";

        Response<Boolean> mockResponse = mock(Response.class);
        when(mockResponse.getValue()).thenReturn(false);
        when(blobClientMock.deleteIfExistsWithResponse(any(), any(), any(), any()))
            .thenReturn(mockResponse);

        Response<Boolean> result = invoiceStorageClient.deleteFile(destination);

        assertNotNull(result);
        assertFalse(result.getValue());
    }

    @Test
    void getInvoiceFileSignedUrlShouldReturnOK() {
        when(blobClientMock.getBlobUrl()).thenReturn("http://localhost:8080");
        when(blobClientMock.generateUserDelegationSas(any(), any())).thenReturn("token");

        String url = invoiceStorageClient.getInvoiceFileSignedUrl("fileId");

        assertNotNull(url);
        assertEquals("http://localhost:8080?token", url);
    }

    @Test
    void getInvoiceFileSignedUrlShouldReturnKO() {
        when(blobClientMock.generateUserDelegationSas(any(), any()))
                .thenThrow(new BlobStorageException("test", null, null));

        assertThrows(ClientException.class,
                () -> invoiceStorageClient.getInvoiceFileSignedUrl("fileId"));
    }

}
