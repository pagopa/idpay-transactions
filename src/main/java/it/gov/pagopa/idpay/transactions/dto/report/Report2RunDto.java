package it.gov.pagopa.idpay.transactions.dto.report;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Report2RunDto {
    String reportId;
    String runId;
}
