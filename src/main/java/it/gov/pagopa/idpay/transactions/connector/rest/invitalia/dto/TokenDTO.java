package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Clock;
import java.time.Instant;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TokenDTO {
    @JsonProperty("token_type")
    private String tokenType;
    @JsonProperty("expires_in")
    private Long expiresIn;
    @JsonProperty("access_token")
    private String accessToken;
    private Instant expiry;

    @JsonIgnore
    public TokenDTO(String accessToken, long expiresIn, Clock clock) {
        this.accessToken = accessToken;
        this.expiresIn = expiresIn;
        this.expiry = Instant.now(clock).plusSeconds(expiresIn);
    }

    public boolean isExpiringSoon(Integer secondsBefore, Clock clock){
        return Instant.now(clock).plusMillis(secondsBefore).isAfter(expiry);
    }
}
