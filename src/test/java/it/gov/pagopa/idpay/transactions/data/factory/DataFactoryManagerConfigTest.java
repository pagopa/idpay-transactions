package it.gov.pagopa.idpay.transactions.data.factory;

import com.azure.resourcemanager.datafactory.DataFactoryManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DataFactoryManagerConfigTest {

    private DataFactoryManagerConfig config;

    @BeforeEach
    void setUp() {
        config = new DataFactoryManagerConfig();

    }

    @Test
    void shouldCreateDataFactoryManager() {
        DataFactoryManager manager = config.dataFactoryManager();
        assertNotNull(manager);
    }

}