package it.gov.pagopa.idpay.transactions.connector.rest.invitalia;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.config.InvitaliaConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.TokenDTO;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;

@EnableConfigurationProperties(InvitaliaConfig.class)
@ContextConfiguration(
        classes = {
                InvitaliaTokenRestClientImpl.class,
                WebClientConfig.class,
                InvitaliaConfig.class,
                AuditUtilities.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.invitalia.token.base-url",
                "app.invitalia.credentials.client-id=CLIENT_ID",
                "app.invitalia.credentials.client-secret=CLIENT_SECRET",
                "app.invitalia.credentials.scope=SCOPE",
                "app.invitalia.credentials.grant-type=GRANT_TYPE",
                "app.invitalia.credentials.tenant-id=TENANT_OK_1",

                "app.invitalia.token.base-url=url",
                "app.invitalia.token.path=/path",
                "app.invitalia.token.retry.delay-millis=2000",
                "app.invitalia.token.retry.max-attempts=10",
                "app.invitalia.token.refresh-before-expiry=60000"
        }
)
class InvitaliaTokenRestClientImplTest extends BaseWireMockTest {
    @Autowired
    private InvitaliaTokenRestClient restClient;

    @Test
    void getToken() {
        TokenDTO result = restClient.getToken().block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals("ACCESS_TOKEN",result.getAccessToken());
    }

}