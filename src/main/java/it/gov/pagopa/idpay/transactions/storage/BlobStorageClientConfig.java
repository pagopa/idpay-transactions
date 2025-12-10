package it.gov.pagopa.idpay.transactions.storage;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BlobStorageClientConfig {

  private final BlobStorageProperties properties;

  public BlobStorageClientConfig(BlobStorageProperties properties) {
    this.properties = properties;
  }

  @Bean
  public BlobServiceClient blobServiceClient() {
    return new BlobServiceClientBuilder()
        .endpoint("https://" + properties.getStorageAccountName() + ".blob.core.windows.net")
        .credential(new DefaultAzureCredentialBuilder().build())
        .buildClient();
  }

  @Bean("invoiceContainerClient")
  public BlobContainerClient blobContainerClient(BlobServiceClient blobServiceClient) {
    return blobServiceClient.getBlobContainerClient(properties.getContainerReference());
  }

  @Bean("csvContainerClient")
  public BlobContainerClient csvContainerClient(BlobServiceClient blobServiceClient){
    return blobServiceClient.getBlobContainerClient(properties.getCsvContainerReference());
  }

}
