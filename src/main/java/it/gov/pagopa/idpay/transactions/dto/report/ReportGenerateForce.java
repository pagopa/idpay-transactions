package it.gov.pagopa.idpay.transactions.dto.report;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReportGenerateForce {
    List<String> reportsId;
}
