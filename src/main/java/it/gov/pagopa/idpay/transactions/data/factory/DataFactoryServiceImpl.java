package it.gov.pagopa.idpay.transactions.data.factory;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.resourcemanager.datafactory.models.CreateRunResponse;
import it.gov.pagopa.idpay.transactions.exception.AzureConnectingErrorException;
import it.gov.pagopa.idpay.transactions.model.Report;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.azure.resourcemanager.datafactory.DataFactoryManager;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class DataFactoryServiceImpl implements DataFactoryService{
    private final DataFactoryManager dataFactoryManager;
    private final String resourceGroup;
    private final String factoryName;
    private final String pipelineName;
    private final int maxRetries;

    public DataFactoryServiceImpl(DataFactoryManager dataFactoryManager,
                                  @Value("${app.data-factory.resource-group}") String resourceGroup,
                                  @Value("${app.data-factory.factory-name}") String factoryName,
                                  @Value("${app.data-factory.pipeline-name}") String pipelineName,
                                  @Value("${app.data-factory.max-retries}") int maxRetries) {
        this.dataFactoryManager = dataFactoryManager;
        this.resourceGroup = resourceGroup;
        this.factoryName = factoryName;
        this.pipelineName = pipelineName;
        this.maxRetries = maxRetries;
    }

    @Override
    public Mono<String> triggerTransactionReportPipeline(Report report) {
        Mono<String> callMono = Mono.fromCallable(() -> {
                    log.info("[CALLING_DATA_FACTORY] Starting pipeline execution for Report {}", report.getId());
                    Response<CreateRunResponse> resp = dataFactoryManager.pipelines().createRunWithResponse(
                            resourceGroup,
                            factoryName,
                            pipelineName,
                            null,
                            false,
                            null,
                            false,
                            createPipelineParameters(report),
                            Context.NONE);

                    int status = resp.getStatusCode();
                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("ADF createRun failed. HTTP status: " + status);
                    }

                    CreateRunResponse body = resp.getValue();
                    if (body == null) {
                        throw new IllegalStateException("ADF createRun returned empty body");
                    }
                    log.info("[CALLING_DATA_FACTORY] Report {} generation request sent successfully. Run ID: {}", report.getId(), body.runId());
                    return body.runId();
                })
                .subscribeOn(Schedulers.boundedElastic());

        return callMono
                .retryWhen(Retry.fixedDelay(maxRetries, Duration.ofSeconds(1))
                        .onRetryExhaustedThrow((spec, signal) ->
                                new AzureConnectingErrorException(
                                        "Failed to trigger ADF pipeline after " + (maxRetries + 1) + " attempts",
                                        signal.failure()
                                )
                        )
                );
    }

    private Map<String, Object> createPipelineParameters(Report report) {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("reportId", report.getId());
        parameters.put("merchantId", report.getMerchantId());
        parameters.put("initiativeId", report.getInitiativeId());
        parameters.put("startDate", report.getStartPeriod());
        parameters.put("endDate", report.getEndPeriod());
        parameters.put("reportName", report.getFileName());

        return parameters;
    }
}
