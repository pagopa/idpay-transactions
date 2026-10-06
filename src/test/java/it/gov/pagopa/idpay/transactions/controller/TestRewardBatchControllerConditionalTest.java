package it.gov.pagopa.idpay.transactions.controller;

import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertFalse;

import it.gov.pagopa.idpay.transactions.service.TestRewardBatchService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class TestRewardBatchControllerConditionalTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestRewardBatchController.class, ServiceConfiguration.class);

    @Test
    void controllerIsAbsentWhenTestSupportIsDisabled() {
        contextRunner.withPropertyValues("app.test-support.enabled=false")
                .run(context -> assertFalse(context.containsBean("testRewardBatchController")));
    }

    @Test
    void controllerIsAbsentWhenTestSupportPropertyIsMissing() {
        contextRunner.run(context -> assertFalse(context.containsBean("testRewardBatchController")));
    }

    @Configuration(proxyBeanMethods = false)
    static class ServiceConfiguration {

        @Bean
        TestRewardBatchService testRewardBatchService() {
            return mock(TestRewardBatchService.class);
        }

    }
}
