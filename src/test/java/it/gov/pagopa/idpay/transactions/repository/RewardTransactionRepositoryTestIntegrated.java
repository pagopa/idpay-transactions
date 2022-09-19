package it.gov.pagopa.idpay.transactions.repository;

import org.springframework.test.context.TestPropertySource;

@TestPropertySource(locations = {
        "classpath:/mongodbEmbeddedDisabled.properties",
        "classpath:/secrets/mongodbConnectionString.properties"
})
public class RewardTransactionRepositoryTestIntegrated extends RewardTransactionRepositoryTest{
}
