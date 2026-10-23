package it.gov.pagopa.idpay.transactions.data.factory;

import it.gov.pagopa.idpay.transactions.model.Report;
import reactor.core.publisher.Mono;
import java.time.LocalDateTime;

public interface DataFactoryService {
    Mono<String> triggerTransactionReportPipeline(Report report);

    Mono<String> triggerUserDetailsReportPipeline(Report report);

    Mono<String> triggerRewardBatchCsvPipeline(String initiativeId, String merchantId, String rewardBatchId, String reportId, String reportName);

    Mono<String> triggerMerchantBatchCsvPipeline(String initiativeId, String merchantId, String reportId, String reportName, LocalDateTime startDate, LocalDateTime endDate);
}
