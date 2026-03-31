package it.gov.pagopa.idpay.transactions.dto.mapper;

import static org.junit.jupiter.api.Assertions.*;

import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.Report;

import java.time.*;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

class ReportMapperTest {

    private ReportMapper mapper;
    private ZoneId zone;

    @BeforeEach
    void setUp() {
        mapper = new ReportMapper();
        zone = ZoneId.systemDefault();
    }

    @Test
    void toDTO_AllFieldsPopulated() {
        Report report = Report.builder()
                .id("report123")
                .initiativeId("initiativeABC")
                .merchantId("merchantXYZ")
                .businessName("BusinessName")
                .fileName("report.csv")
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(LocalDate.of(2026, 2, 1).atStartOfDay(zone).toInstant())
                .endPeriod(LocalDate.of(2026, 2, 28).atTime(23, 59).atZone(zone).toInstant())
                .requestDate(LocalDate.of(2026, 2, 10).atTime( 10, 0).atZone(zone).toInstant())
                .elaborationDate(LocalDate.of(2026, 2, 10).atTime(12, 0).atZone(zone).toInstant())
                .operatorLevel(RewardBatchAssignee.L1)
                .reportType(ReportType.MERCHANT_TRANSACTIONS)
                .build();

        ReportDTO dto = mapper.toDTO(report);

        assertNotNull(dto);
        assertEquals("report123", dto.getId());
        assertEquals("initiativeABC", dto.getInitiativeId());
        assertEquals("merchantXYZ", dto.getMerchantId());
        assertEquals("BusinessName", dto.getBusinessName());
        assertEquals("report.csv", dto.getFileName());
        assertEquals(ReportStatus.INSERTED, dto.getReportStatus());

        assertEquals(LocalDate.of(2026, 2, 1).atStartOfDay(zone).toInstant(), dto.getStartPeriod());
        assertEquals(LocalDate.of(2026, 2, 28).atTime(23, 59).atZone(zone).toInstant(), dto.getEndPeriod());
        assertEquals(LocalDate.of(2026, 2, 10).atTime(10, 0).atZone(zone).toInstant(), dto.getRequestDate());
        assertEquals(LocalDate.of(2026, 2, 10).atTime(12, 0).atZone(zone).toInstant(), dto.getElaborationDate());

        assertEquals(RewardBatchAssignee.L1, dto.getOperatorLevel());
        assertEquals(ReportType.MERCHANT_TRANSACTIONS, dto.getReportType());
    }

    @Test
    void toDTO_NullReport_ReturnsNull() {
        ReportDTO dto = mapper.toDTO(null);
        assertNull(dto);
    }

    @Test
    void toListDTO_PopulatedPage() {
        Report report1 = Report.builder()
                .id("r1")
                .initiativeId("i1")
                .merchantId("m1")
                .fileName("file1.csv")
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(LocalDate.of(2026, 2, 1).atStartOfDay(zone).toInstant())
                .endPeriod(LocalDate.of(2026, 2, 28).atTime(23, 59).atZone(zone).toInstant())
                .build();

        Report report2 = Report.builder()
                .id("r2")
                .initiativeId("i2")
                .merchantId("m2")
                .fileName("file2.csv")
                .reportStatus(ReportStatus.GENERATED)
                .startPeriod(LocalDate.of(2026, 3, 1).atStartOfDay(zone).toInstant())
                .endPeriod(LocalDate.of(2026, 3, 31).atTime(23, 59).atZone(zone).toInstant())
                .build();

        Page<Report> page = new PageImpl<>(List.of(report1, report2), PageRequest.of(0, 10), 2);

        ReportListDTO listDTO = mapper.toListDTO(page);

        assertNotNull(listDTO);
        assertEquals(2, listDTO.getReports().size());
        assertEquals(0, listDTO.getPage());
        assertEquals(10, listDTO.getSize());
        assertEquals(2, listDTO.getTotalElements());
        assertEquals(1, listDTO.getTotalPages());

        ReportDTO dto1 = listDTO.getReports().get(0);
        assertEquals("r1", dto1.getId());
        assertEquals("file1.csv", dto1.getFileName());
        assertEquals(ReportStatus.INSERTED, dto1.getReportStatus());

        ReportDTO dto2 = listDTO.getReports().get(1);
        assertEquals("r2", dto2.getId());
        assertEquals("file2.csv", dto2.getFileName());
        assertEquals(ReportStatus.GENERATED, dto2.getReportStatus());
    }

    @Test
    void toListDTO_EmptyPage() {
        Page<Report> emptyPage = new PageImpl<>(List.of(), PageRequest.of(0, 10), 0);
        ReportListDTO listDTO = mapper.toListDTO(emptyPage);

        assertNotNull(listDTO);
        assertTrue(listDTO.getReports().isEmpty());
        assertEquals(0, listDTO.getTotalElements());
        assertEquals(0, listDTO.getTotalPages());
    }
}