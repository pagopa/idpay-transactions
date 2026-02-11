package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.service.ReportService;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@RestController
@Slf4j
public class ReportControllerImpl implements ReportController {

    private final ReportService reportService;
    private final ReportMapper reportMapper;

    public ReportControllerImpl(ReportService reportService, ReportMapper reportMapper) {
        this.reportService = reportService;
        this.reportMapper = reportMapper;
    }

    @Override
    public Mono<ReportListDTO> getTransactionsReports(
            String merchantId,
            String organizationRole,
            String rewardBatchAssignee,
            String initiativeId,
            Pageable pageable
    ) {
        log.info("[GET_TRANSACTIONS_REPORTS] Request received for initiative: {}", initiativeId);

        return reportService.getTransactionsReports(merchantId, organizationRole, rewardBatchAssignee, initiativeId, pageable)
                .flatMap(page -> Mono.just(reportMapper.toListDTO(page)));
    }


    @Override
    public Mono<ReportDTO> generateReport(String merchantId,
                                          String organizationRole,
                                          String initiativeId,
                                          ReportRequest request
    ) {
            return reportService.generateReport(merchantId, organizationRole, initiativeId, request);
    }
}
