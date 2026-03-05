package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.Instant;

@Data
public class TokenDTO {
    @JsonProperty("token_type")
    private String tokenType;
    @JsonProperty("expires_in")
    private Long expiresIn;
    @JsonProperty("access_token")
    private String accessToken;
    private Instant expiry;

    public TokenDTO(String accessToken, long expiresIn) {
        this.accessToken = accessToken;
        this.expiresIn = expiresIn;
        this.expiry = Instant.now().plusSeconds(expiresIn);
    }

    public boolean isExpiringSoon(Integer secondsBefore){
        return Instant.now().plusMillis(secondsBefore).isAfter(expiry);
    }
}
