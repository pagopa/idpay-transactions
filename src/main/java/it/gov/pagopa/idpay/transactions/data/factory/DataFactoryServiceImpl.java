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
    private final String pipelineTransactionReportName;
    private final String pipelineUserDetailsReportName;
    private final String pipelineRewardBatchCsvName;
    private final int maxRetries;

    public DataFactoryServiceImpl(DataFactoryManager dataFactoryManager,
                                  @Value("${app.data-factory.resource-group}") String resourceGroup,
                                  @Value("${app.data-factory.factory-name}") String factoryName,
                                  @Value("${app.data-factory.pipeline-transaction-report-name}") String pipelineTransactionReportName,
                                  @Value("${app.data-factory.pipeline-user-details-report-name}") String pipelineUserDetailsReportName,
                                  @Value("${app.data-factory.pipeline-reward-batch-csv-name}") String pipelineRewardBatchCsvName,
                                  @Value("${app.data-factory.max-retries}") int maxRetries) {
        this.dataFactoryManager = dataFactoryManager;
        this.resourceGroup = resourceGroup;
        this.factoryName = factoryName;
        this.pipelineTransactionReportName = pipelineTransactionReportName;
        this.pipelineUserDetailsReportName = pipelineUserDetailsReportName;
        this.pipelineRewardBatchCsvName = pipelineRewardBatchCsvName;
        this.maxRetries = maxRetries;
    }

    @Override
    public Mono<String> triggerTransactionReportPipeline(Report report) {
        return triggerPipeline(pipelineTransactionReportName, createReportPipelineParameters(report), report.getId());
    }

    @Override
    public Mono<String> triggerUserDetailsReportPipeline(Report report) {
        return triggerPipeline(pipelineUserDetailsReportName, createReportPipelineParameters(report), report.getId());
    }

    @Override
    public Mono<String> triggerRewardBatchCsvPipeline(String initiativeId, String merchantId, String rewardBatchId, String reportName) {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("initiativeId", initiativeId);
        parameters.put("merchantId", merchantId);
        parameters.put("rewardBatchId", rewardBatchId);
        parameters.put("reportName", reportName);
        return triggerPipeline(pipelineRewardBatchCsvName, parameters, rewardBatchId);
    }

    private Map<String, Object> createReportPipelineParameters(Report report) {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("reportId", report.getId());
        parameters.put("merchantId", report.getMerchantId());
        parameters.put("initiativeId", report.getInitiativeId());
        parameters.put("startDate", report.getStartPeriod());
        parameters.put("endDate", report.getEndPeriod());
        parameters.put("reportName", report.getFileName());

        return parameters;
    }

    private Mono<String> triggerPipeline(String pipelineName, Map<String, Object> parameters, String contextId) {
        Mono<String> callMono = Mono.fromCallable(() -> {
                    log.info("[CALLING_DATA_FACTORY] Starting pipeline {} execution for {}", pipelineName, contextId);
                    Response<CreateRunResponse> resp = dataFactoryManager.pipelines().createRunWithResponse(
                            resourceGroup,
                            factoryName,
                            pipelineName,
                            null,
                            false,
                            null,
                            false,
                            parameters,
                            Context.NONE);

                    int status = resp.getStatusCode();
                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("ADF createRun failed. HTTP status: " + status);
                    }

                    CreateRunResponse body = resp.getValue();
                    if (body == null) {
                        throw new IllegalStateException("ADF createRun returned empty body");
                    }
                    log.info("[CALLING_DATA_FACTORY] Pipeline {} triggered for {}. Run ID: {}", pipelineName, contextId, body.runId());
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
}
