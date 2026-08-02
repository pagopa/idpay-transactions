package it.gov.pagopa.idpay.transactions.data.factory;


import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.resourcemanager.datafactory.DataFactoryManager;
import com.azure.resourcemanager.datafactory.models.CreateRunResponse;
import com.azure.resourcemanager.datafactory.models.Pipelines;
import it.gov.pagopa.idpay.transactions.exception.AzureConnectingErrorException;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataFactoryServiceImplTest {

    @Mock
    private DataFactoryManager dataFactoryManager;

    @Mock
    private Pipelines pipelines;

    @Mock
    private Response<CreateRunResponse> response;

    @Mock
    private CreateRunResponse createRunResponse;

    private DataFactoryServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new DataFactoryServiceImpl(
                dataFactoryManager,
                "resourceGroup",
                "factoryName",
                "pipelineTransactionReportname",
                "pipelineUserDetailsReportname",
                0
        );
    }

    @Test
    void triggerTransactionReport_Pipeline_success() {

        LocalDateTime now = LocalDateTime.now();
        Report report = Report.builder()
                .id("REPORT_ID")
                .merchantId("MERCHANT")
                .initiativeId("INIT")
                .startPeriod(now)
                .endPeriod(now)
                .fileName("file.csv").build();

        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(),
                anyString(),
                anyString(),
                isNull(),
                eq(false),
                isNull(),
                eq(false),
                anyMap(),
                eq(Context.NONE)
        )).thenReturn(response);

        when(response.getStatusCode()).thenReturn(200);
        when(response.getValue()).thenReturn(createRunResponse);
        when(createRunResponse.runId()).thenReturn("RUN_ID");

        StepVerifier.create(service.triggerTransactionReportPipeline(report))
                .expectNext("RUN_ID")
                .verifyComplete();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(),
                anyString(),
                anyString(),
                any(),
                anyBoolean(),
                any(),
                anyBoolean(),
                anyMap(),
                any()
        );
    }

    @Test
    void triggerTransactionReport_Pipeline_error_shouldThrowAzureConnectingError() {

        Report report = mock(Report.class);
        when(report.getId()).thenReturn("REPORT_ID");

        when(dataFactoryManager.pipelines()).thenReturn(pipelines);

        when(pipelines.createRunWithResponse(
                anyString(),
                anyString(),
                anyString(),
                any(),
                anyBoolean(),
                any(),
                anyBoolean(),
                anyMap(),
                any()
        )).thenThrow(new RuntimeException("Connection error"));

        StepVerifier.create(service.triggerTransactionReportPipeline(report))
                .expectError(AzureConnectingErrorException.class)
                .verify();
    }

    @Test
    void triggerTransactionReportPipelineShouldRejectInformationalStatus() {
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(199);

        StepVerifier.create(service.triggerTransactionReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 1 attempts", exception.getMessage());
                    assertEquals("ADF createRun failed. HTTP status: 199", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }

    @Test
    void triggerTransactionReportPipelineShouldFailAfterOneAttemptForNonSuccessfulStatus() {
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(503);

        StepVerifier.create(service.triggerTransactionReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 1 attempts", exception.getMessage());
                    assertEquals("ADF createRun failed. HTTP status: 503", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }

    @Test
    void triggerTransactionReportPipelineShouldRecoverAfterOneRetryForNonSuccessfulStatus() {
        DataFactoryServiceImpl retryingService = new DataFactoryServiceImpl(
                dataFactoryManager,
                "resourceGroup",
                "factoryName",
                "pipelineTransactionReportname",
                "pipelineUserDetailsReportname",
                1
        );
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(503, 200);
        when(response.getValue()).thenReturn(createRunResponse);
        when(createRunResponse.runId()).thenReturn("TRANSACTION_RUN_ID");

        StepVerifier.create(retryingService.triggerTransactionReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectNext("TRANSACTION_RUN_ID")
                .verifyComplete();

        verify(pipelines, times(2)).createRunWithResponse(
                eq("resourceGroup"), eq("factoryName"), eq("pipelineTransactionReportname"),
                isNull(), eq(false), isNull(), eq(false), anyMap(), eq(Context.NONE)
        );
    }

    @Test
    void triggerTransactionReportPipelineShouldFailAfterOneAttemptForEmptyResponseBody() {
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getValue()).thenReturn(null);

        StepVerifier.create(service.triggerTransactionReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 1 attempts", exception.getMessage());
                    assertEquals("ADF createRun returned empty body", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }

    @Test
    void triggerUserDetailsReportPipelineShouldUseUserDetailsPipelineAndReportParameters() {
        LocalDateTime start = LocalDateTime.of(2026, 8, 1, 9, 0);
        LocalDateTime end = LocalDateTime.of(2026, 8, 2, 18, 0);
        Report report = Report.builder()
                .id("REPORT_ID")
                .merchantId("MERCHANT")
                .initiativeId("INIT")
                .startPeriod(start)
                .endPeriod(end)
                .fileName("user-details.csv")
                .build();

        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(201);
        when(response.getValue()).thenReturn(createRunResponse);
        when(createRunResponse.runId()).thenReturn("USER_DETAILS_RUN_ID");

        StepVerifier.create(service.triggerUserDetailsReportPipeline(report))
                .expectNext("USER_DETAILS_RUN_ID")
                .verifyComplete();

        verify(pipelines).createRunWithResponse(
                eq("resourceGroup"),
                eq("factoryName"),
                eq("pipelineUserDetailsReportname"),
                isNull(),
                eq(false),
                isNull(),
                eq(false),
                argThat(parameters -> parameters.equals(Map.of(
                        "reportId", "REPORT_ID",
                        "merchantId", "MERCHANT",
                        "initiativeId", "INIT",
                        "startDate", start,
                        "endDate", end,
                        "reportName", "user-details.csv"
                ))),
                eq(Context.NONE)
        );
    }

    @Test
    void triggerUserDetailsReportPipelineShouldFailAfterOneAttemptForNonSuccessfulStatus() {
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(503);

        StepVerifier.create(service.triggerUserDetailsReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 1 attempts", exception.getMessage());
                    assertEquals("ADF createRun failed. HTTP status: 503", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }

    @Test
    void triggerUserDetailsReportPipelineShouldRejectInformationalStatus() {
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(199);

        StepVerifier.create(service.triggerUserDetailsReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 1 attempts", exception.getMessage());
                    assertEquals("ADF createRun failed. HTTP status: 199", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }

    @Test
    void triggerUserDetailsReportPipelineShouldRetryOnceForNonSuccessfulStatus() {
        DataFactoryServiceImpl retryingService = new DataFactoryServiceImpl(
                dataFactoryManager,
                "resourceGroup",
                "factoryName",
                "pipelineTransactionReportname",
                "pipelineUserDetailsReportname",
                1
        );
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(503);

        StepVerifier.create(retryingService.triggerUserDetailsReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 2 attempts", exception.getMessage());
                    assertEquals("ADF createRun failed. HTTP status: 503", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(2)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }

    @Test
    void triggerUserDetailsReportPipelineShouldRecoverAfterOneRetryForNonSuccessfulStatus() {
        DataFactoryServiceImpl retryingService = new DataFactoryServiceImpl(
                dataFactoryManager,
                "resourceGroup",
                "factoryName",
                "pipelineTransactionReportname",
                "pipelineUserDetailsReportname",
                1
        );
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(503, 200);
        when(response.getValue()).thenReturn(createRunResponse);
        when(createRunResponse.runId()).thenReturn("USER_DETAILS_RUN_ID");

        StepVerifier.create(retryingService.triggerUserDetailsReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectNext("USER_DETAILS_RUN_ID")
                .verifyComplete();

        verify(pipelines, times(2)).createRunWithResponse(
                eq("resourceGroup"), eq("factoryName"), eq("pipelineUserDetailsReportname"),
                isNull(), eq(false), isNull(), eq(false), anyMap(), eq(Context.NONE)
        );
    }

    @Test
    void triggerUserDetailsReportPipelineShouldFailAfterOneAttemptForEmptyResponseBody() {
        when(dataFactoryManager.pipelines()).thenReturn(pipelines);
        when(pipelines.createRunWithResponse(
                anyString(), anyString(), anyString(), isNull(), eq(false),
                isNull(), eq(false), anyMap(), eq(Context.NONE)
        )).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getValue()).thenReturn(null);

        StepVerifier.create(service.triggerUserDetailsReportPipeline(Report.builder().id("REPORT_ID").build()))
                .expectErrorSatisfies(error -> {
                    AzureConnectingErrorException exception = (AzureConnectingErrorException) error;
                    assertEquals("Failed to trigger ADF pipeline after 1 attempts", exception.getMessage());
                    assertEquals("ADF createRun returned empty body", exception.getCause().getMessage());
                })
                .verify();

        verify(pipelines, times(1)).createRunWithResponse(
                anyString(), anyString(), anyString(), any(), anyBoolean(),
                any(), anyBoolean(), anyMap(), any()
        );
    }
}