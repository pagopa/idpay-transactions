package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import it.gov.pagopa.idpay.transactions.service.ReportService;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

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
}
