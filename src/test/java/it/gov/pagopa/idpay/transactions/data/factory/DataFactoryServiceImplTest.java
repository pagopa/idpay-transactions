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
                "pipelineName",
                0
        );
    }

    @Test
    void generateReport_success() {

        LocalDateTime now = LocalDateTime.now();
        Report report = Report.builder()
                .id("REPORT_ID")
                .merchantId("MERCHANT")
                .initiativeId("INIT")
                .startPeriod(now)
                .endPeriod(now)
                .fileName("file.csv").build();

        // mock Azure chain
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

        StepVerifier.create(service.generateReport(report))
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
    void generateReport_error_shouldThrowAzureConnectingError() {

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

        StepVerifier.create(service.generateReport(report))
                .expectError(AzureConnectingErrorException.class)
                .verify();
    }
}