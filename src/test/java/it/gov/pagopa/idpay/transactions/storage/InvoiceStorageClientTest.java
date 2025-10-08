package it.gov.pagopa.idpay.transactions.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import it.gov.pagopa.common.web.exception.ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
}
