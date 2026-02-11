package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.repository.ReportRepository;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY;

@Slf4j
@Service
public class ReportServiceImpl implements ReportService {

    private final ReportRepository reportRepository;

    public ReportServiceImpl(ReportRepository reportRepository) {
        this.reportRepository = reportRepository;
    }

    @Override
    public Mono<Page<Report>> getTransactionsReports(
            String merchantId,
            String organizationRole,
            String rewardBatchAssignee,
            String initiativeId,
            Pageable pageable
    ) {

        boolean callerIsOperator = organizationRole != null && organizationRole.startsWith("operator");

        if (merchantId == null && organizationRole == null) {
            log.warn("[GET_TRANSACTIONS_REPORTS] Missing mandatory filters: merchantId and organizationRole are null");
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY,
                    ERROR_MESSAGE_MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY
            );
        }

        log.info("[GET_TRANSACTIONS_REPORTS] Fetching reports for initiative: {}, merchant: {}, role: {}",
                Utilities.sanitizeString(initiativeId),
                merchantId != null ? Utilities.sanitizeString(merchantId) : "null",
                organizationRole != null ? Utilities.sanitizeString(organizationRole) : "null");

        return reportRepository.findReportsCombined(
                        merchantId,
                        organizationRole,
                        rewardBatchAssignee,
                        initiativeId,
                        callerIsOperator,
                        pageable
                )
                .collectList()
                .zipWith(reportRepository.countReportsCombined(
                        merchantId,
                        organizationRole,
                        rewardBatchAssignee,
                        initiativeId,
                        callerIsOperator
                ))
                .flatMap(tuple -> Mono.just(new PageImpl<>(tuple.getT1(), pageable, tuple.getT2())));
    }

}
