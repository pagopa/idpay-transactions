package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.service.ReportService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REPORT_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_REPORT_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@WebFluxTest(controllers = ReportControllerImpl.class)
class ReportControllerImplTest {

    @Autowired
    protected WebTestClient webClient;

    @MockitoBean
    private ReportService reportService;

    @MockitoBean
    private ReportMapper reportMapper;

    private static final String MERCHANT_ID = "280b09dc-76d9-3b93-bf21-68a5094bc322";
    private static final String INITIATIVE_ID = "68dd003ccce8c534d1da22bc";

    @Test
    void getReports_ReturnsReports_Success() {
        Report report = Report.builder()
                .id("report1")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .businessName("BusinessName")
                .fileName("transactions_report_january.csv")
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(LocalDateTime.of(2026, 2, 1, 0, 0))
                .endPeriod(LocalDateTime.of(2026, 2, 28, 23, 59))
                .requestDate(LocalDateTime.of(2026, 2, 10, 10, 0))
                .elaborationDate(LocalDateTime.of(2026, 2, 10, 12, 0))
                .operatorLevel(RewardBatchAssignee.L1)
                .build();

        Page<Report> servicePage = new PageImpl<>(List.of(report), PageRequest.of(0, 10), 1);

        ReportDTO reportDTO = ReportDTO.builder()
                .id(report.getId())
                .initiativeId(report.getInitiativeId())
                .merchantId(report.getMerchantId())
                .businessName(report.getBusinessName())
                .fileName(report.getFileName())
                .reportStatus(report.getReportStatus())
                .startPeriod(report.getStartPeriod())
                .endPeriod(report.getEndPeriod())
                .requestDate(report.getRequestDate())
                .elaborationDate(report.getElaborationDate())
                .operatorLevel(report.getOperatorLevel())
                .build();

        ReportListDTO listDTO = ReportListDTO.builder()
                .reports(List.of(reportDTO))
                .page(0)
                .size(10)
                .totalElements(1)
                .totalPages(1)
                .build();

        when(reportService.getTransactionsReports(
                eq(MERCHANT_ID),
                isNull(),
                eq(INITIATIVE_ID),
                any(Pageable.class)
        )).thenReturn(Mono.just(servicePage));

        when(reportMapper.toListDTO(servicePage)).thenReturn(listDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/merchant/portal/initiatives/{initiativeId}/reports")
                        .queryParam("page", 0)
                        .queryParam("size", 10)
                        .build(INITIATIVE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody(ReportListDTO.class)
                .value(response -> {
                    assertNotNull(response);
                    assertEquals(1, response.getReports().size());
                    ReportDTO dto = response.getReports().get(0);
                    assertEquals("report1", dto.getId());
                    assertEquals("transactions_report_january.csv", dto.getFileName());
                    assertEquals(ReportStatus.INSERTED, dto.getReportStatus());
                    assertEquals(LocalDateTime.of(2026, 2, 1, 0, 0), dto.getStartPeriod());
                    assertEquals(LocalDateTime.of(2026, 2, 28, 23, 59), dto.getEndPeriod());
                    assertEquals(1, response.getTotalElements());
                    assertEquals(1, response.getTotalPages());
                    assertEquals(10, response.getSize());
                });

        verify(reportService, times(1))
                .getTransactionsReports(eq(MERCHANT_ID), isNull(), eq(INITIATIVE_ID), any(Pageable.class));
        verify(reportMapper, times(1)).toListDTO(servicePage);
    }

    @Test
    void getReports_ServiceFails_InternalServerError() {
        when(reportService.getTransactionsReports(
                eq(MERCHANT_ID),
                isNull(),
                eq(INITIATIVE_ID),
                any(Pageable.class)
        )).thenReturn(Mono.error(new RuntimeException("Service failure")));

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/merchant/portal/initiatives/{initiativeId}/reports")
                        .queryParam("page", 0)
                        .queryParam("size", 10)
                        .build(INITIATIVE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().is5xxServerError();

        verify(reportService, times(1))
                .getTransactionsReports(eq(MERCHANT_ID), isNull(), eq(INITIATIVE_ID), any(Pageable.class));
    }

    @Test
    void generateReport_ReturnsReport_Success() {
        ReportRequest request = ReportRequest.builder()
                .startPeriod(LocalDateTime.of(2026, 2, 1, 0, 0))
                .endPeriod(LocalDateTime.of(2026, 2, 28, 23, 59))
                .reportType(ReportType.MERCHANT_TRANSACTIONS)
                .build();

        ReportDTO reportDTO = ReportDTO.builder()
                .id("generatedReport1")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .fileName("generated_report.csv")
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(request.getStartPeriod())
                .endPeriod(request.getEndPeriod())
                .operatorLevel(null)
                .build();

        when(reportService.generateReport(
                eq(MERCHANT_ID),
                isNull(),
                eq(INITIATIVE_ID),
                eq(request)
        )).thenReturn(Mono.just(reportDTO));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reports", INITIATIVE_ID)
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(ReportDTO.class)
                .value(response -> {
                    assertNotNull(response);
                    assertEquals("generatedReport1", response.getId());
                    assertEquals("generated_report.csv", response.getFileName());
                    assertEquals(ReportStatus.INSERTED, response.getReportStatus());
                    assertEquals(request.getStartPeriod(), response.getStartPeriod());
                    assertEquals(request.getEndPeriod(), response.getEndPeriod());
                });

        verify(reportService, times(1))
                .generateReport(eq(MERCHANT_ID), isNull(), eq(INITIATIVE_ID), eq(request));
    }

    @Test
    void generateReport_ServiceFails_InternalServerError() {
        ReportRequest request = ReportRequest.builder()
                .startPeriod(LocalDateTime.of(2026, 2, 1, 0, 0))
                .endPeriod(LocalDateTime.of(2026, 2, 28, 23, 59))
                .reportType(ReportType.MERCHANT_TRANSACTIONS)
                .build();

        when(reportService.generateReport(
                eq(MERCHANT_ID),
                isNull(),
                eq(INITIATIVE_ID),
                eq(request)
        )).thenReturn(Mono.error(new RuntimeException("Service failure")));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reports", INITIATIVE_ID)
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().is5xxServerError();

        verify(reportService, times(1))
                .generateReport(eq(MERCHANT_ID), isNull(), eq(INITIATIVE_ID), eq(request));
    }

    @Test
    void patchReport_Success() {
        PatchReportRequest request = PatchReportRequest.builder()
                .reportStatus(ReportStatus.GENERATED)
                .build();

        Report report = Report.builder()
                .id("report123")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .reportStatus(ReportStatus.GENERATED)
                .build();

        ReportDTO reportDTO = ReportDTO.builder()
                .id(report.getId())
                .initiativeId(report.getInitiativeId())
                .merchantId(report.getMerchantId())
                .reportStatus(report.getReportStatus())
                .build();

        when(reportService.patchReport(eq(INITIATIVE_ID), eq("report123"), any(PatchReportRequest.class)))
                .thenReturn(Mono.just(reportDTO));

        webClient.patch()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reports/{reportId}",
                        INITIATIVE_ID, "report123")
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(ReportDTO.class)
                .value(response -> {
                    assertNotNull(response);
                    assertEquals("report123", response.getId());
                    assertEquals(ReportStatus.GENERATED, response.getReportStatus());
                });

        verify(reportService, times(1))
                .patchReport(eq(INITIATIVE_ID), eq("report123"), any(PatchReportRequest.class));
    }

    @Test
    void patchReport_NotFound() {
        PatchReportRequest request = PatchReportRequest.builder()
                .reportStatus(ReportStatus.GENERATED)
                .build();

        when(reportService.patchReport(eq(INITIATIVE_ID), eq("missingReport"), any()))
                .thenReturn(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        REPORT_NOT_FOUND,
                        ERROR_MESSAGE_REPORT_NOT_FOUND.formatted("missingReport", INITIATIVE_ID)
                )));

        webClient.patch()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reports/{reportId}",
                        INITIATIVE_ID, "missingReport")
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().isNotFound();

        verify(reportService, times(1))
                .patchReport(eq(INITIATIVE_ID), eq("missingReport"), any());
    }



}
