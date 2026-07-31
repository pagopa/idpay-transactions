package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.data.factory.DataFactoryService;
import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.dto.report.ReportGenerateForce;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.persistence.port.ReportPersistencePort;
import it.gov.pagopa.idpay.transactions.storage.ReportTransactionsBlobServiceImpl;
import it.gov.pagopa.idpay.transactions.storage.ReportUserDetailsBlobServiceImpl;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ReportServiceImplTest {

    @Mock private ReportPersistencePort reportPort;
    @Mock private MerchantRestClient merchantRestClient;
    @Mock private ReportMapper reportMapper;
    @Mock private ReportTransactionsBlobServiceImpl transactionBlobService;
    @Mock private ReportUserDetailsBlobServiceImpl userDetailsBlobService;
    @Mock private DataFactoryService dataFactoryService;
    private ReportServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new ReportServiceImpl(
                90, reportPort, merchantRestClient, reportMapper, transactionBlobService,
                userDetailsBlobService, dataFactoryService);
    }

    @Test
    void merchantReportsUseMerchantScopeAndMatchingCount() {
        Report report = report("merchant-report", ReportType.MERCHANT_TRANSACTIONS);
        Pageable requested = PageRequest.of(1, 2);
        Pageable expected = PageRequest.of(1, 2, Sort.by(Sort.Direction.DESC, "requestDate"));
        when(reportPort.findReports(
                eq("merchant"), eq(null), eq("initiative"),
                eq(ReportType.MERCHANT_TRANSACTIONS), eq(expected))).thenReturn(Flux.just(report));
        when(reportPort.countReports(
                "merchant", null, "initiative", ReportType.MERCHANT_TRANSACTIONS))
                .thenReturn(Mono.just(3L));

        StepVerifier.create(service.getReports(
                        "merchant", null, "initiative", ReportType.USER_DETAILS, requested))
                .assertNext(page -> {
                    assertEquals(3, page.getTotalElements());
                    assertEquals("merchant-report", page.getContent().getFirst().getId());
                })
                .verifyComplete();
    }

    @Test
    void operatorUserDetailsReportsUseOperatorScopeAndMatchingCount() {
        Report report = report("operator-report", ReportType.USER_DETAILS);
        Pageable expected = PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "requestDate"));
        when(reportPort.findReports(
                null, "operator1", "initiative", ReportType.USER_DETAILS, expected))
                .thenReturn(Flux.just(report));
        when(reportPort.countReports(null, "operator1", "initiative", ReportType.USER_DETAILS))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getUserDetailsReports("operator1", "initiative", PageRequest.of(0, 10)))
                .assertNext(page -> assertEquals("operator-report", page.getContent().getFirst().getId()))
                .verifyComplete();
    }

    @Test
    void patchLoadsWithinInitiativeScopeBeforeSaving() {
        Report report = report("report", ReportType.USER_DETAILS);
        PatchReportRequest request = new PatchReportRequest();
        request.setReportStatus(ReportStatus.GENERATED);
        ReportDTO dto = ReportDTO.builder().id("report").build();
        when(reportPort.findByIdAndInitiativeId("report", "initiative")).thenReturn(Mono.just(report));
        when(reportPort.save(report)).thenReturn(Mono.just(report));
        when(reportMapper.toDTO(report)).thenReturn(dto);

        StepVerifier.create(service.patchReport("initiative", "report", request))
                .expectNext(dto)
                .verifyComplete();

        verify(reportPort).findByIdAndInitiativeId("report", "initiative");
        verify(reportPort).save(report);
        assertEquals(ReportStatus.GENERATED, report.getReportStatus());
    }

    @Test
    void invalidReportQueriesFailBeforePersistenceAccess() {
        assertThrows(ClientExceptionWithBody.class,
                () -> service.getReports(null, null, "initiative", null, PageRequest.of(0, 10)));
        assertThrows(ClientExceptionWithBody.class,
                () -> service.getTransactionsReports(null, null, "initiative", PageRequest.of(0, 10)));
    }

    @Test
    void reportListValidationRejectsConflictingAndInvalidOperatorScopes() {
        assertThrows(ClientExceptionWithBody.class, () -> service.getTransactionsReports(
                "merchant", "operator1", "initiative", PageRequest.of(0, 10)));
        assertThrows(ClientExceptionWithBody.class, () -> service.getTransactionsReports(
                null, "admin", "initiative", PageRequest.of(0, 10)));
        assertThrows(ClientExceptionWithBody.class, () -> service.getUserDetailsReports(
                " ", "initiative", PageRequest.of(0, 10)));
    }

    @Test
    void generatesMerchantReportPersistsItAndTriggersPipeline() {
        ReportRequest request = validRequest(ReportType.MERCHANT_TRANSACTIONS);
        MerchantDetailDTO merchant = new MerchantDetailDTO();
        merchant.setBusinessName("Business");
        Report saved = report("report", ReportType.MERCHANT_TRANSACTIONS);
        ReportDTO dto = ReportDTO.builder().id("report").build();
        when(merchantRestClient.getMerchantDetail("merchant", "initiative")).thenReturn(Mono.just(merchant));
        when(reportPort.save(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.just(saved));
        when(dataFactoryService.triggerTransactionReportPipeline(saved)).thenReturn(Mono.just("run"));
        when(reportMapper.toDTO(saved)).thenReturn(dto);

        StepVerifier.create(service.generateMerchantTransactionsReport(
                        "merchant", "operator1", "initiative", request))
                .expectNext(dto).verifyComplete();

        verify(reportPort).save(org.mockito.ArgumentMatchers.argThat(created ->
                created.getMerchantId().equals("merchant")
                        && created.getReportStatus() == ReportStatus.INSERTED
                        && created.getReportType() == ReportType.MERCHANT_TRANSACTIONS));
        verify(dataFactoryService).triggerTransactionReportPipeline(saved);
    }

    @Test
    void merchantPipelineFailureMarksThePersistedReportFailed() {
        ReportRequest request = validRequest(ReportType.MERCHANT_TRANSACTIONS);
        MerchantDetailDTO merchant = new MerchantDetailDTO();
        merchant.setBusinessName("Business");
        Report saved = report("report", ReportType.MERCHANT_TRANSACTIONS);
        ReportDTO dto = ReportDTO.builder().id("report").build();
        when(merchantRestClient.getMerchantDetail("merchant", "initiative")).thenReturn(Mono.just(merchant));
        when(reportPort.save(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.just(saved));
        when(dataFactoryService.triggerTransactionReportPipeline(saved)).thenReturn(Mono.error(
                new it.gov.pagopa.idpay.transactions.exception.AzureConnectingErrorException("storage", new RuntimeException())));
        when(reportMapper.toDTO(saved)).thenReturn(dto);

        StepVerifier.create(service.generateMerchantTransactionsReport(
                        "merchant", "operator1", "initiative", request))
                .expectNext(dto).verifyComplete();

        org.junit.jupiter.api.Assertions.assertEquals(ReportStatus.FAILED, saved.getReportStatus());
        verify(reportPort, org.mockito.Mockito.times(2)).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void generationValidatesPeriodsBeforeCallingPorts() {
        ReportRequest invalid = new ReportRequest();
        invalid.setReportType(ReportType.MERCHANT_TRANSACTIONS);
        invalid.setStartPeriod(LocalDateTime.now().minusDays(1));
        invalid.setEndPeriod(LocalDateTime.now().minusDays(2));

        StepVerifier.create(service.generateMerchantTransactionsReport(
                        "merchant", "operator1", "initiative", invalid))
                .expectError(ClientExceptionWithBody.class).verify();

        ReportRequest tooLong = validRequest(ReportType.USER_DETAILS);
        tooLong.setStartPeriod(LocalDateTime.now().minusDays(100));
        StepVerifier.create(service.generateMerchantTransactionsReport(
                        "merchant", "operator1", "initiative", tooLong))
                .expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void generatesUserDetailsReportAndTriggersItsPipeline() {
        ReportRequest request = validRequest(ReportType.USER_DETAILS);
        Report saved = report("report", ReportType.USER_DETAILS);
        ReportDTO dto = ReportDTO.builder().id("report").build();
        when(reportPort.save(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.just(saved));
        when(dataFactoryService.triggerUserDetailsReportPipeline(saved)).thenReturn(Mono.just("run"));
        when(reportMapper.toDTO(saved)).thenReturn(dto);

        StepVerifier.create(service.generateUserDetailsReport("operator2", "initiative", request))
                .expectNext(dto).verifyComplete();

        verify(dataFactoryService).triggerUserDetailsReportPipeline(saved);
    }

    @Test
    void patchAndForceGenerationSurfaceScopedNotFoundAndUsePersistedReports() {
        when(reportPort.findByIdAndInitiativeId("missing", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(service.patchReport("initiative", "missing", new PatchReportRequest()))
                .expectError(ClientExceptionWithBody.class).verify();

        Report report = report("report", ReportType.MERCHANT_TRANSACTIONS);
        when(reportPort.findAllById(List.of("report"))).thenReturn(Flux.just(report));
        when(dataFactoryService.triggerTransactionReportPipeline(report)).thenReturn(Mono.just("run"));
        ReportGenerateForce forceRequest = new ReportGenerateForce();
        forceRequest.setReportsId(List.of("report"));
        StepVerifier.create(service.forceGenerateReports(forceRequest))
                .assertNext(runs -> assertEquals("report", runs.getFirst().getReportId()))
                .verifyComplete();
    }

    @Test
    void downloadsGeneratedMerchantAndUserDetailsReportsFromTheirScopedBlobs() {
        Report merchant = report("merchant-report", ReportType.MERCHANT_TRANSACTIONS);
        merchant.setMerchantId("merchant");
        merchant.setFileName("merchant.csv");
        merchant.setReportStatus(ReportStatus.GENERATED);
        when(reportPort.findByIdAndInitiativeIdAndMerchantId("merchant-report", "initiative", "merchant"))
                .thenReturn(Mono.just(merchant));
        when(transactionBlobService.getFileSignedUrl(
                "initiative/initiative/merchant/merchant/report/merchant.csv")).thenReturn("merchant-url");
        StepVerifier.create(service.downloadTransactionsReport(
                        "merchant", null, "initiative", "merchant-report"))
                .assertNext(result -> assertEquals("merchant-url", result.getReportUrl())).verifyComplete();

        Report user = report("user-report", ReportType.USER_DETAILS);
        user.setFileName("users.csv");
        user.setReportStatus(ReportStatus.GENERATED);
        when(reportPort.findByIdAndInitiativeId("user-report", "initiative")).thenReturn(Mono.just(user));
        when(userDetailsBlobService.getFileSignedUrl("initiative/initiative/report/users.csv"))
                .thenReturn("users-url");
        StepVerifier.create(service.downloadUserDetailsReports("operator3", "initiative", "user-report"))
                .assertNext(result -> assertEquals("users-url", result.getReportUrl())).verifyComplete();
    }

    @Test
    void downloadValidationRejectsMissingScopesAndIncompleteReports() {
        StepVerifier.create(service.downloadTransactionsReport(null, null, "initiative", "report"))
                .expectError(ClientExceptionWithBody.class).verify();
        StepVerifier.create(service.downloadUserDetailsReports("admin", "initiative", "report"))
                .expectError(ClientExceptionWithBody.class).verify();

        Report notGenerated = report("report", ReportType.MERCHANT_TRANSACTIONS);
        notGenerated.setMerchantId("merchant");
        notGenerated.setFileName("file.csv");
        when(reportPort.findByIdAndInitiativeIdAndMerchantId("report", "initiative", "merchant"))
                .thenReturn(Mono.just(notGenerated));
        StepVerifier.create(service.downloadTransactionsReport("merchant", null, "initiative", "report"))
                .expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void getReportsDispatchesUserDetailsAndReturnsAnEmptyPage() {
        Pageable expected = PageRequest.of(0, 5, Sort.by(Sort.Direction.DESC, "requestDate"));
        when(reportPort.findReports(null, "operator1", "initiative", ReportType.USER_DETAILS, expected))
                .thenReturn(Flux.empty());
        when(reportPort.countReports(null, "operator1", "initiative", ReportType.USER_DETAILS))
                .thenReturn(Mono.just(0L));

        StepVerifier.create(service.getReports(
                        null, "operator1", "initiative", ReportType.USER_DETAILS, PageRequest.of(0, 5)))
                .assertNext(page -> assertEquals(0, page.getTotalElements()))
                .verifyComplete();
    }

    @Test
    void userDetailsPipelineFailureIsPersistedAsFailed() {
        Report saved = report("report", ReportType.USER_DETAILS);
        ReportDTO dto = ReportDTO.builder().id("report").build();
        when(reportPort.save(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.just(saved));
        when(dataFactoryService.triggerUserDetailsReportPipeline(saved)).thenReturn(Mono.error(
                new it.gov.pagopa.idpay.transactions.exception.AzureConnectingErrorException("storage", new RuntimeException())));
        when(reportMapper.toDTO(saved)).thenReturn(dto);

        StepVerifier.create(service.generateUserDetailsReport(
                        "operator1", "initiative", validRequest(ReportType.USER_DETAILS)))
                .expectNext(dto).verifyComplete();

        assertEquals(ReportStatus.FAILED, saved.getReportStatus());
        verify(reportPort, org.mockito.Mockito.times(2)).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void downloadReportsDispatchesByPersistedTypeAndRejectsUnknownType() {
        Report merchant = report("merchant-report", ReportType.MERCHANT_TRANSACTIONS);
        merchant.setMerchantId("merchant");
        merchant.setFileName("merchant.csv");
        merchant.setReportStatus(ReportStatus.GENERATED);
        when(reportPort.findByIdAndInitiativeId("merchant-report", "initiative")).thenReturn(Mono.just(merchant));
        when(reportPort.findByIdAndInitiativeIdAndMerchantId("merchant-report", "initiative", "merchant"))
                .thenReturn(Mono.just(merchant));
        when(transactionBlobService.getFileSignedUrl(
                "initiative/initiative/merchant/merchant/report/merchant.csv")).thenReturn("url");
        StepVerifier.create(service.downloadReports("merchant", null, "initiative", "merchant-report"))
                .assertNext(result -> assertEquals("url", result.getReportUrl())).verifyComplete();

        when(reportPort.findByIdAndInitiativeId("missing", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(service.downloadReports(null, "operator1", "initiative", "missing"))
                .expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void downloadGeneratedReportsRejectsMissingFilenamesAndNotFoundScopes() {
        Report missingFilename = report("report", ReportType.USER_DETAILS);
        missingFilename.setReportStatus(ReportStatus.GENERATED);
        when(reportPort.findByIdAndInitiativeId("report", "initiative")).thenReturn(Mono.just(missingFilename));
        StepVerifier.create(service.downloadUserDetailsReports("operator1", "initiative", "report"))
                .expectError(ClientExceptionWithBody.class).verify();

        when(reportPort.findByIdAndInitiativeIdAndMerchantId("missing", "initiative", "merchant"))
                .thenReturn(Mono.empty());
        StepVerifier.create(service.downloadTransactionsReport("merchant", null, "initiative", "missing"))
                .expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void reportEntryPointDispatchesBothTypesAndValidatesMerchantRequirement() {
        ReportServiceImpl dispatcher = org.mockito.Mockito.spy(service);
        ReportDTO dto = ReportDTO.builder().id("report").build();
        ReportRequest merchantRequest = validRequest(ReportType.MERCHANT_TRANSACTIONS);
        ReportRequest userRequest = validRequest(ReportType.USER_DETAILS);
        org.mockito.Mockito.doReturn(Mono.just(dto)).when(dispatcher)
                .generateMerchantTransactionsReport("merchant", "operator1", "initiative", merchantRequest);
        org.mockito.Mockito.doReturn(Mono.just(dto)).when(dispatcher)
                .generateUserDetailsReport("operator1", "initiative", userRequest);

        StepVerifier.create(dispatcher.generateReport("merchant", "operator1", "initiative", merchantRequest))
                .expectNext(dto).verifyComplete();
        StepVerifier.create(dispatcher.generateReport(null, "operator1", "initiative", userRequest))
                .expectNext(dto).verifyComplete();
        StepVerifier.create(service.generateReport(null, "operator1", "initiative", merchantRequest))
                .expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void operatorTransactionDownloadUsesInitiativeScopedLookup() {
        Report report = report("report", ReportType.MERCHANT_TRANSACTIONS);
        report.setMerchantId("merchant");
        report.setFileName("report.csv");
        report.setReportStatus(ReportStatus.GENERATED);
        when(reportPort.findByIdAndInitiativeId("report", "initiative")).thenReturn(Mono.just(report));
        when(transactionBlobService.getFileSignedUrl(
                "initiative/initiative/merchant/merchant/report/report.csv")).thenReturn("url");

        StepVerifier.create(service.downloadTransactionsReport(null, "operator1", "initiative", "report"))
                .assertNext(result -> assertEquals("url", result.getReportUrl())).verifyComplete();
    }

    @Test
    void invalidUserDetailsRoleAndPeriodAreRejected() {
        assertThrows(ClientExceptionWithBody.class,
                () -> service.getUserDetailsReports("admin", "initiative", PageRequest.of(0, 10)));
        ReportRequest invalid = validRequest(ReportType.USER_DETAILS);
        invalid.setEndPeriod(LocalDateTime.now());
        StepVerifier.create(service.generateUserDetailsReport("operator1", "initiative", invalid))
                .expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void downloadReportsDispatchesUserDetailsType() {
        Report user = report("user", ReportType.USER_DETAILS);
        user.setFileName("users.csv");
        user.setReportStatus(ReportStatus.GENERATED);
        when(reportPort.findByIdAndInitiativeId("user", "initiative")).thenReturn(Mono.just(user));
        when(userDetailsBlobService.getFileSignedUrl("initiative/initiative/report/users.csv")).thenReturn("url");

        StepVerifier.create(service.downloadReports(null, "operator1", "initiative", "user"))
                .assertNext(result -> assertEquals("url", result.getReportUrl())).verifyComplete();
    }

    private static ReportRequest validRequest(ReportType type) {
        ReportRequest request = new ReportRequest();
        request.setReportType(type);
        request.setStartPeriod(LocalDateTime.now().minusDays(2));
        request.setEndPeriod(LocalDateTime.now().minusDays(1));
        return request;
    }

    private static Report report(String id, ReportType type) {
        return Report.builder()
                .id(id)
                .initiativeId("initiative")
                .reportStatus(ReportStatus.INSERTED)
                .requestDate(LocalDateTime.parse("2026-01-01T00:00:00"))
                .reportType(type)
                .build();
    }
}
