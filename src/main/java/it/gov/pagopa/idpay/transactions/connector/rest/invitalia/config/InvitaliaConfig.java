package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.invitalia")
public class InvitaliaConfig {
    private Credentials credentials;
    private Token token;


    @Data
    public static class Credentials {
        private String clientId;
        private String clientSecret;
        private String scope;
        private String grantType;
        private String tenantId;
    }

    @Data
    public static class Token {
        private String baseUrl;
        private String path;
        private TokenRetry retry;
        private Integer refreshBeforeExpiry;
    }

        @Data
    public static class TokenRetry {
        private Integer delayMillis;
        private Integer maxAttempts;
    }
}
