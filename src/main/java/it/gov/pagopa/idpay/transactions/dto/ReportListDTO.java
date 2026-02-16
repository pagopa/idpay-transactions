package it.gov.pagopa.idpay.transactions.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ReportListDTO {
    private List<ReportDTO> reports;
    private int page;
    private int size;
    private int totalElements;
    private int totalPages;
}
