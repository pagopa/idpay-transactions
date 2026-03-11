package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestPropertySource( properties = {
        "app.invitalia.credentials.client-id=CLIENT_ID",
        "app.invitalia.credentials.client-secret=CLIENT_SECRET",
        "app.invitalia.credentials.scope=SCOPE",
        "app.invitalia.credentials.grant-type=grant_type",
        "app.invitalia.credentials.tenant-id=TENANT_ID",

        "app.invitalia.token.base-url=url",
        "app.invitalia.token.path=/path",
        "app.invitalia.token.retry.delay-millis=2000",
        "app.invitalia.token.retry.max-attempts=10",
        "app.invitalia.token.refresh-before-expiry=60000"

})
@ExtendWith(SpringExtension.class)
@EnableConfigurationProperties(value = InvitaliaConfig.class)
class InvitaliaConfigTest {

    @Value("${app.invitalia.credentials.client-id}")
    private String clientId;

    @Value("${app.invitalia.credentials.client-secret}")
    private String clientSecret;

    @Value("${app.invitalia.credentials.scope}")
    private String clientScope;

    @Value("${app.invitalia.credentials.grant-type}")
    private String grantType;

    @Value("${app.invitalia.credentials.tenant-id}")
    private String tenantId;

    @Value("${app.invitalia.token.base-url}")
    private String tokenBaseUrl;

    @Value("${app.invitalia.token.path}")
    private String tokenPath;

    @Value("${app.invitalia.token.retry.delay-millis}")
    private Integer delayMillis;

    @Value("${app.invitalia.token.retry.max-attempts}")
    private Integer maxAttempts;

    @Value("${app.invitalia.token.refresh-before-expiry}")
    private Integer expiry;

    @Autowired
    private InvitaliaConfig config;

    @Test
    void getCredentials(){
        InvitaliaConfig.Credentials credentials = config.getCredentials();

        assertNotNull(credentials);
        assertEquals(credentials.getClientId(), clientId);
        assertEquals(credentials.getClientSecret(), clientSecret);
        assertEquals(credentials.getScope(), clientScope);
        assertEquals(credentials.getGrantType(), grantType);
        assertEquals(credentials.getTenantId(), tenantId);
    }


    @Test
    void getToken(){
        InvitaliaConfig.Token token = config.getToken();

        assertNotNull(token);
        assertEquals(token.getBaseUrl(), tokenBaseUrl);
        assertEquals(token.getPath(), tokenPath);
        InvitaliaConfig.TokenRetry tokenRetry = token.getRetry();
        assertNotNull(tokenRetry);
        assertEquals(tokenRetry.getDelayMillis(), delayMillis);
        assertEquals(tokenRetry.getMaxAttempts(), maxAttempts);
        assertEquals(token.getRefreshBeforeExpiry(), expiry);
    }


}