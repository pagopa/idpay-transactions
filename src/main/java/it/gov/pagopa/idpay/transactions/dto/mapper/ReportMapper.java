package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ReportMapper {

    public ReportDTO toDTO(Report report) {
        if (report == null) {
            return null;
        }

        return ReportDTO.builder()
                .id(report.getId())
                .initiativeId(report.getInitiativeId())
                .reportStatus(report.getReportStatus())
                .startPeriod(report.getStartPeriod())
                .endPeriod(report.getEndPeriod())
                .merchantId(report.getMerchantId())
                .businessName(report.getBusinessName())
                .requestDate(report.getRequestDate())
                .elaborationDate(report.getElaborationDate())
                .operatorLevel(report.getOperatorLevel())
                .fileName(report.getFileName())
                .reportType(report.getReportType())
                .build();
    }

    public ReportListDTO toListDTO(Page<Report> page) {
        List<ReportDTO> dtoList = page.getContent()
                .stream()
                .map(this::toDTO)
                .collect(Collectors.toList());

        return ReportListDTO.builder()
                .reports(dtoList)
                .page(page.getNumber())
                .size(page.getSize())
                .totalElements((int) page.getTotalElements())
                .totalPages(page.getTotalPages())
                .build();
    }
}
