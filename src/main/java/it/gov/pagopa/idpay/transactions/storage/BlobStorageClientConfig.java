package it.gov.pagopa.idpay.transactions.storage;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerAsyncClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BlobStorageClientConfig {

  private final BlobStorageProperties properties;

  public BlobStorageClientConfig(BlobStorageProperties properties) {
    this.properties = properties;
  }

  @Bean
  public BlobServiceAsyncClient blobServiceAsyncClient() {
    return new BlobServiceClientBuilder()
        .endpoint("https://" + properties.getStorageAccountName() + ".blob.core.windows.net")
        .credential(new DefaultAzureCredentialBuilder().build())
        .buildAsyncClient();
  }

  @Bean("invoiceContainerClient")
  public BlobContainerAsyncClient blobContainerClient(BlobServiceAsyncClient blobServiceClient) {
    return blobServiceClient.getBlobContainerAsyncClient(properties.getContainerReference());
  }

  @Bean("rewardBatchesContainerClient")
  public BlobContainerAsyncClient rewardBatchesContainerClient(BlobServiceAsyncClient blobServiceClient){
    return blobServiceClient.getBlobContainerAsyncClient(properties.getCsvContainerReference());
  }

  @Bean("reportsTransactionsContainerClient")
  public BlobContainerAsyncClient reportsTransactionsContainerClient(BlobServiceAsyncClient blobServiceClient){
    return blobServiceClient.getBlobContainerAsyncClient(properties.getReportsTransactionsContainerReference());
  }

  @Bean("reportsUserDetailsContainerClient")
  public BlobContainerAsyncClient reportsUserDetailsContainerClient(BlobServiceAsyncClient blobServiceClient){
    return blobServiceClient.getBlobContainerAsyncClient(properties.getReportsUserDetailsContainerReference());
  }
}
