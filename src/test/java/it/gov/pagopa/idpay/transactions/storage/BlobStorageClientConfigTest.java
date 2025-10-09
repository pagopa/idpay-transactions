package it.gov.pagopa.idpay.transactions.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlobStorageClientConfigTest {

  private BlobStorageClientConfig blobStorageClientConfig;

  @BeforeEach
  void setUp() {
    BlobStorageProperties properties = new BlobStorageProperties();
    properties.setStorageAccountName("storageaccount");
    properties.setContainerReference("containerreference");
    properties.setInvoiceTokenDurationSeconds(60);

    blobStorageClientConfig = new BlobStorageClientConfig(properties);
  }

  @Test
  void testBlobServiceClient() {
    BlobServiceClient serviceClient = blobStorageClientConfig.blobServiceClient();
    assertNotNull(serviceClient);
    assert(serviceClient.getAccountUrl().contains("storageaccount"));
  }

  @Test
  void testBlobContainerClient() {
    BlobServiceClient serviceClient = blobStorageClientConfig.blobServiceClient();
    BlobContainerClient containerClient = blobStorageClientConfig.blobContainerClient(serviceClient);
    assertNotNull(containerClient);
    assert(containerClient.getBlobContainerName().equals("containerreference"));
  }
}
