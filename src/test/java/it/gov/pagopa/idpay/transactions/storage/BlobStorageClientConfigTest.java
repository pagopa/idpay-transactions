package it.gov.pagopa.idpay.transactions.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
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
  void testBlobServiceAsyncClient() {
    BlobServiceAsyncClient serviceClient = blobStorageClientConfig.blobServiceAsyncClient();
    assertNotNull(serviceClient);
    assertTrue(serviceClient.getAccountUrl().contains("storageaccount"));
  }

  @Test
  void testBlobContainerAsyncClient() {
    BlobServiceAsyncClient serviceClient = blobStorageClientConfig.blobServiceAsyncClient();
    BlobContainerAsyncClient containerClient = blobStorageClientConfig.blobContainerClient(serviceClient);
    assertNotNull(containerClient);
    assert(containerClient.getBlobContainerName().equals("containerreference"));
  }

  @Test
  void testReportsTransactionsContainerClient() {
    BlobServiceAsyncClient serviceClient = blobStorageClientConfig.blobServiceAsyncClient();

    BlobStorageProperties properties = new BlobStorageProperties();
    properties.setReportsTransactionsContainerReference("reportsTransactionsContainer");

    BlobStorageClientConfig config = new BlobStorageClientConfig(properties);

    BlobContainerAsyncClient reportsClient = config.reportsTransactionsContainerClient(serviceClient);
    assertNotNull(reportsClient);
    assert(reportsClient.getBlobContainerName().equals("reportsTransactionsContainer"));
  }

  @Test
  void testReportsUserDetailsContainerClient() {
    BlobServiceAsyncClient serviceClient = blobStorageClientConfig.blobServiceAsyncClient();

    BlobStorageProperties properties = new BlobStorageProperties();
    properties.setReportsUserDetailsContainerReference("reportsUserDetailsContainer");

    BlobStorageClientConfig config = new BlobStorageClientConfig(properties);

    BlobContainerAsyncClient reportsClient = config.reportsUserDetailsContainerClient(serviceClient);
    assertNotNull(reportsClient);
    assert(reportsClient.getBlobContainerName().equals("reportsUserDetailsContainer"));
  }

}
