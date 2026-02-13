package it.gov.pagopa.idpay.transactions.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DownloadReportResponseDTO {

    private String reportUrl;
}
