package it.gov.pagopa.idpay.transactions.dto;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ReportRequest {

    @NotNull
    private Instant startPeriod;
    @NotNull
    private Instant endPeriod;
    @NotNull
    private ReportType reportType;

}

