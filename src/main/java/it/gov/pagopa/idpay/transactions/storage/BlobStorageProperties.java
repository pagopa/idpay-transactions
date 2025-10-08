package it.gov.pagopa.idpay.transactions.storage;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
@ConfigurationProperties(prefix = "blobstorage")
public class BlobStorageProperties {

  private String storageAccountName;
  private String containerReference;
  private Integer invoiceTokenDurationSeconds;
}
