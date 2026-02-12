package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.repository.ReportRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReportServiceImplTest {

    @Mock
    private ReportRepository reportRepository;

    @Mock
    private MerchantRestClient merchantRestClient;

    @Mock
    private ReportMapper reportMapper;

    private ReportServiceImpl service;

    private static final String MERCHANT_ID = "M1";
    private static final String INITIATIVE_ID = "INIT1";
    private static final String ORGANIZATION_ROLE = "operator1";

    @BeforeEach
    void setup() {
        service = new ReportServiceImpl(reportRepository, merchantRestClient, reportMapper);
    }

    @Test
    void getTransactionsReports_returnsPage_success() {
        Report report = Report.builder()
                .id("R1")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .businessName("Business")
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(LocalDateTime.of(2026, 2, 1, 0, 0))
                .endPeriod(LocalDateTime.of(2026, 2, 28, 23, 59))
                .operatorLevel(RewardBatchAssignee.L1)
                .fileName("report.csv")
                .requestDate(LocalDateTime.now())
                .elaborationDate(LocalDateTime.now())
                .build();

        Pageable pageable = PageRequest.of(0, 10);
        List<Report> reports = List.of(report);

        when(reportRepository.findReportsCombined(
                eq(MERCHANT_ID),
                eq(ORGANIZATION_ROLE),
                isNull(),
                eq(INITIATIVE_ID),
                eq(true),
                eq(pageable)
        )).thenReturn(Flux.fromIterable(reports));

        when(reportRepository.countReportsCombined(
                eq(MERCHANT_ID),
                eq(ORGANIZATION_ROLE),
                isNull(),
                eq(INITIATIVE_ID),
                eq(true)
        )).thenReturn(Mono.just(1L));

        StepVerifier.create(service.getTransactionsReports(MERCHANT_ID, ORGANIZATION_ROLE, null, INITIATIVE_ID, pageable))
                .assertNext(page -> {
                    assertNotNull(page);
                    assertEquals(1, page.getTotalElements());
                    assertEquals(1, page.getContent().size());
                    assertEquals("R1", page.getContent().get(0).getId());
                })
                .verifyComplete();

        verify(reportRepository, times(1)).findReportsCombined(any(), any(), any(), any(), anyBoolean(), any());
        verify(reportRepository, times(1)).countReportsCombined(any(), any(), any(), any(), anyBoolean());
    }

    @Test
    void getTransactionsReports_throwsBadRequest_whenMerchantIdAndRoleNull() {
        Pageable pageable = PageRequest.of(0, 10);

        ClientExceptionWithBody ex = assertThrows(ClientExceptionWithBody.class,
                () -> service.getTransactionsReports(null, null, null, INITIATIVE_ID, pageable));

        assertEquals(400, ex.getHttpStatus().value());
    }

    @Test
    void getTransactionsReports_returnsEmpty_whenNoReports() {
        Pageable pageable = PageRequest.of(0, 10);

        when(reportRepository.findReportsCombined(any(), any(), any(), any(), anyBoolean(), any()))
                .thenReturn(Flux.empty());
        when(reportRepository.countReportsCombined(any(), any(), any(), any(), anyBoolean()))
                .thenReturn(Mono.just(0L));

        StepVerifier.create(service.getTransactionsReports(MERCHANT_ID, ORGANIZATION_ROLE, null, INITIATIVE_ID, pageable))
                .assertNext(page -> {
                    assertNotNull(page);
                    assertTrue(page.getContent().isEmpty());
                    assertEquals(0, page.getTotalElements());
                })
                .verifyComplete();
    }

    @Test
    void getTransactionsReports_onlyOrganizationRole_success() {
        Pageable pageable = PageRequest.of(0, 10);

        Report report = Report.builder()
                .id("R2")
                .initiativeId(INITIATIVE_ID)
                .merchantId(null)
                .businessName("Business")
                .reportStatus(ReportStatus.INSERTED)
                .operatorLevel(RewardBatchAssignee.L1)
                .fileName("report2.csv")
                .requestDate(LocalDateTime.now())
                .elaborationDate(LocalDateTime.now())
                .build();

        when(reportRepository.findReportsCombined(
                isNull(),
                eq(ORGANIZATION_ROLE),
                isNull(),
                eq(INITIATIVE_ID),
                eq(true),
                eq(pageable)
        )).thenReturn(Flux.just(report));

        when(reportRepository.countReportsCombined(
                isNull(),
                eq(ORGANIZATION_ROLE),
                isNull(),
                eq(INITIATIVE_ID),
                eq(true)
        )).thenReturn(Mono.just(1L));

        StepVerifier.create(service.getTransactionsReports(null, ORGANIZATION_ROLE, null, INITIATIVE_ID, pageable))
                .assertNext(page -> {
                    assertNotNull(page);
                    assertEquals(1, page.getTotalElements());
                    assertEquals("R2", page.getContent().get(0).getId());
                })
                .verifyComplete();
    }

    @Test
    void getTransactionsReports_onlyMerchantId_success() {
        Pageable pageable = PageRequest.of(0, 10);

        Report report = Report.builder()
                .id("R3")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .businessName("Business")
                .reportStatus(ReportStatus.INSERTED)
                .operatorLevel(RewardBatchAssignee.L1)
                .fileName("report3.csv")
                .requestDate(LocalDateTime.now())
                .elaborationDate(LocalDateTime.now())
                .build();

        when(reportRepository.findReportsCombined(
                eq(MERCHANT_ID),
                isNull(),
                isNull(),
                eq(INITIATIVE_ID),
                eq(false),
                eq(pageable)
        )).thenReturn(Flux.just(report));

        when(reportRepository.countReportsCombined(
                eq(MERCHANT_ID),
                isNull(),
                isNull(),
                eq(INITIATIVE_ID),
                eq(false)
        )).thenReturn(Mono.just(1L));

        StepVerifier.create(service.getTransactionsReports(MERCHANT_ID, null, null, INITIATIVE_ID, pageable))
                .assertNext(page -> {
                    assertNotNull(page);
                    assertEquals(1, page.getTotalElements());
                    assertEquals("R3", page.getContent().get(0).getId());
                })
                .verifyComplete();
    }


    @Test
    void generateReport_merchantTransactions_success() {
        ReportRequest request = new ReportRequest();
        request.setReportType(ReportType.MERCHANT_TRANSACTIONS);
        request.setStartPeriod(LocalDateTime.now().minusDays(10));
        request.setEndPeriod(LocalDateTime.now());
        request.setOperatorLevel(RewardBatchAssignee.L1);

        ReportDTO expectedDto = ReportDTO.builder().id("R1").build();

        ReportServiceImpl spyService = spy(service);
        doReturn(Mono.just(expectedDto))
                .when(spyService)
                .generateMerchantTransactionsReport(any(), any(), any(), any());

        StepVerifier.create(spyService.generateReport(MERCHANT_ID, ORGANIZATION_ROLE, INITIATIVE_ID, request))
                .expectNext(expectedDto)
                .verifyComplete();

        verify(spyService, times(1))
                .generateMerchantTransactionsReport(MERCHANT_ID, ORGANIZATION_ROLE, INITIATIVE_ID, request);
    }

    @Test
    void generateMerchantTransactionsReport_success() {
        ReportRequest request = new ReportRequest();
        request.setStartPeriod(LocalDateTime.now().minusDays(5));
        request.setEndPeriod(LocalDateTime.now());
        request.setOperatorLevel(RewardBatchAssignee.L1);
        request.setReportType(ReportType.MERCHANT_TRANSACTIONS);

        MerchantDetailDTO merchant = new MerchantDetailDTO();
        merchant.setBusinessName("Test Business");

        Report savedReport = Report.builder()
                .id("R100")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .businessName("Test Business")
                .fileName("Report_01012026120000")
                .reportStatus(ReportStatus.INSERTED)
                .build();

        ReportDTO mappedDto = ReportDTO.builder()
                .id("R100")
                .fileName(savedReport.getFileName())
                .businessName(savedReport.getBusinessName())
                .build();

        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(merchant));

        when(reportRepository.save(any()))
                .thenReturn(Mono.just(savedReport));

        when(reportMapper.toDTO(savedReport))
                .thenReturn(mappedDto);

        StepVerifier.create(service.generateMerchantTransactionsReport(MERCHANT_ID, ORGANIZATION_ROLE, INITIATIVE_ID, request))
                .expectNext(mappedDto)
                .verifyComplete();

        verify(merchantRestClient).getMerchantDetail(MERCHANT_ID, INITIATIVE_ID);
        verify(reportRepository).save(any(Report.class));
        verify(reportMapper).toDTO(savedReport);
    }

    @Test
    void generateMerchantTransactionsReport_merchantDetailError() {
        ReportRequest request = new ReportRequest();
        request.setStartPeriod(LocalDateTime.now().minusDays(5));
        request.setEndPeriod(LocalDateTime.now());
        request.setOperatorLevel(RewardBatchAssignee.L1);
        request.setReportType(ReportType.MERCHANT_TRANSACTIONS);

        RuntimeException remoteError = new RuntimeException("Merchant not found");

        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID))
                .thenReturn(Mono.error(remoteError));

        StepVerifier.create(service.generateMerchantTransactionsReport(MERCHANT_ID, ORGANIZATION_ROLE, INITIATIVE_ID, request))
                .expectErrorMatches(err -> err instanceof RuntimeException &&
                        err.getMessage().equals("Merchant not found"))
                .verify();

        verify(merchantRestClient).getMerchantDetail(MERCHANT_ID, INITIATIVE_ID);
        verifyNoInteractions(reportRepository);
        verifyNoInteractions(reportMapper);
    }
    @Test
    void generateMerchantTransactionsReport_saveError() {
        ReportRequest request = new ReportRequest();
        request.setStartPeriod(LocalDateTime.now().minusDays(5));
        request.setEndPeriod(LocalDateTime.now());
        request.setOperatorLevel(RewardBatchAssignee.L1);
        request.setReportType(ReportType.MERCHANT_TRANSACTIONS);

        MerchantDetailDTO merchant = new MerchantDetailDTO();
        merchant.setBusinessName("Test Business");

        when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(merchant));

        RuntimeException saveError = new RuntimeException("DB error");

        when(reportRepository.save(any()))
                .thenReturn(Mono.error(saveError));

        StepVerifier.create(service.generateMerchantTransactionsReport(MERCHANT_ID, ORGANIZATION_ROLE, INITIATIVE_ID, request))
                .expectErrorMatches(err -> err instanceof RuntimeException &&
                        err.getMessage().equals("DB error"))
                .verify();

        verify(merchantRestClient).getMerchantDetail(MERCHANT_ID, INITIATIVE_ID);
        verify(reportRepository).save(any());
        verifyNoInteractions(reportMapper);
    }
    @Test
    void generateMerchantTransactionsReport_fileNameGeneratedCorrectly() {
        ReportRequest request = new ReportRequest();
        request.setStartPeriod(LocalDateTime.of(2026, 1, 1, 0, 0));
        request.setEndPeriod(LocalDateTime.of(2026, 1, 31, 23, 59));
        request.setOperatorLevel(RewardBatchAssignee.L1);
        request.setReportType(ReportType.MERCHANT_TRANSACTIONS);

        MerchantDetailDTO merchant = new MerchantDetailDTO();
        merchant.setBusinessName("Business");

        LocalDateTime fixedNow = LocalDateTime.of(2026, 2, 1, 12, 30, 45);

        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class, CALLS_REAL_METHODS)) {
            mocked.when(LocalDateTime::now).thenReturn(fixedNow);

            when(merchantRestClient.getMerchantDetail(MERCHANT_ID, INITIATIVE_ID))
                    .thenReturn(Mono.just(merchant));

            ArgumentCaptor<Report> captor = ArgumentCaptor.forClass(Report.class);

            Report saved = Report.builder()
                    .id("R200")
                    .fileName("Report_01022026123045")
                    .build();

            when(reportRepository.save(any())).thenReturn(Mono.just(saved));
            when(reportMapper.toDTO(saved)).thenReturn(ReportDTO.builder().id("R200").fileName(saved.getFileName()).build());

            StepVerifier.create(service.generateMerchantTransactionsReport(MERCHANT_ID, ORGANIZATION_ROLE, INITIATIVE_ID, request))
                    .assertNext(dto -> assertEquals("Report_01022026123045", dto.getFileName()))
                    .verifyComplete();

            verify(reportRepository).save(captor.capture());
            assertEquals("Report_01022026123045", captor.getValue().getFileName());
        }
    }


}
