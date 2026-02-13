package it.gov.pagopa.idpay.transactions.data.factory;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.azure.resourcemanager.datafactory.DataFactoryManager;


@Configuration
public class DataFactoryManagerConfig {
    @Bean
    public DataFactoryManager dataFactoryManager () {
        DefaultAzureCredential azureCredentials = new DefaultAzureCredentialBuilder().build();
        AzureProfile azureProfile = new AzureProfile(AzureEnvironment.AZURE);
        return DataFactoryManager.authenticate(azureCredentials, azureProfile);
    }
}
