package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class InitiativeDetailDTO {
    private String initiativeName;
    private String status;
    private String description;
    private String ruleDescription;
    private LocalDate onboardingStartDate;
    private LocalDate onboardingEndDate;
    private LocalDate fruitionStartDate;
    private LocalDate fruitionEndDate;
    private String privacyLink;
    private String tcLink;
    private String logoURL;
    private LocalDateTime updateDate;
    private String serviceId;
}
