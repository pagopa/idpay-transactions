package it.gov.pagopa.idpay.transactions.data.factory;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.azure.resourcemanager.datafactory.DataFactoryManager;


@Configuration
public class DataFactoryManagerConfig {
    private final String tenantId;
    private final String subscriptionId;

    public DataFactoryManagerConfig(@Value("${app.data-factory.tenant-id}") String tenantId,
                                    @Value("${app.data-factory.subscription-id}") String subscriptionId) {
        this.tenantId = tenantId;
        this.subscriptionId = subscriptionId;
    }

    @Bean
    public DataFactoryManager dataFactoryManager () {
        DefaultAzureCredential azureCredentials = new DefaultAzureCredentialBuilder().build();
        AzureProfile azureProfile = new AzureProfile(tenantId, subscriptionId, AzureEnvironment.AZURE);
        return DataFactoryManager.authenticate(azureCredentials, azureProfile);
    }
}
