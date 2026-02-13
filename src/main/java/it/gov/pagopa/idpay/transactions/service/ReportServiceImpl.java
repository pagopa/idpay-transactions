package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.repository.ReportRepository;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Slf4j
@Service
public class ReportServiceImpl implements ReportService {

    private final ReportRepository reportRepository;

    private final MerchantRestClient merchantRestClient;

    private final ReportMapper reportMapper;

    public ReportServiceImpl(ReportRepository reportRepository, MerchantRestClient merchantRestClient, ReportMapper reportMapper) {
        this.reportRepository = reportRepository;
        this.merchantRestClient = merchantRestClient;
        this.reportMapper = reportMapper;
    }

    private static final List<String> ALLOWED_ROLES = List.of(
            "operator1", "operator2", "operator3"
    );
    private static final DateTimeFormatter FILE_NAME_FORMAT = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss");

    @Override
    public Mono<Page<Report>> getTransactionsReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            Pageable pageable
    ) {

        if (merchantId == null && organizationRole == null) {
            log.warn("[GET_TRANSACTIONS_REPORTS] Missing mandatory filters: merchantId and organizationRole are null");
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY,
                    ERROR_MESSAGE_MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY
            );
        }

        if (merchantId != null && organizationRole != null) {
            log.warn("[GET_TRANSACTIONS_REPORTS] Both merchantId and organizationRole provided");
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    MERCHANT_ID_AND_ORGANIZATION_ROLE_CANNOT_COEXIST,
                    ERROR_MESSAGE_MERCHANT_ID_AND_ORGANIZATION_ROLE_CANNOT_COEXIST
            );
        }

        if (organizationRole != null &&
                ALLOWED_ROLES.stream().noneMatch(role -> role.equalsIgnoreCase(organizationRole))) {

            log.warn("[GET_TRANSACTIONS_REPORTS] Invalid organizationRole: {}",
                    Utilities.sanitizeString(organizationRole));

            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    INVALID_ORGANIZATION_ROLE,
                    ERROR_MESSAGE_INVALID_ORGANIZATION_ROLE
            );
        }

        log.info("[GET_TRANSACTIONS_REPORTS] Fetching reports for initiative: {}, merchant: {}, role: {}",
                Utilities.sanitizeString(initiativeId),
                merchantId != null ? Utilities.sanitizeString(merchantId) : "null",
                organizationRole != null ? Utilities.sanitizeString(organizationRole) : "null");

        return reportRepository.findReportsCombined(
                        merchantId,
                        organizationRole,
                        initiativeId,
                        pageable
                )
                .collectList()
                .zipWith(reportRepository.countReportsCombined(
                        merchantId,
                        organizationRole,
                        initiativeId
                ))
                .flatMap(tuple -> Mono.just(new PageImpl<>(tuple.getT1(), pageable, tuple.getT2())));
    }

    @Override
    public Mono<ReportDTO> generateReport(String merchantId,
                                   String organizationRole,
                                   String initiativeId,
                                   ReportRequest request){
        if (ReportType.MERCHANT_TRANSACTIONS.equals(request.getReportType())) {
            log.info("[GET_MERCHANT_REPORT] Requested report with MerchantId = {}, startPeriod = {}, endPeriod = {}",
                    Utilities.sanitizeString(merchantId),
                    request.getStartPeriod(),
                    request.getEndPeriod());
            return generateMerchantTransactionsReport(merchantId, organizationRole, initiativeId, request);
    }
        return Mono.empty();
}


    public Mono<ReportDTO> generateMerchantTransactionsReport(String merchantId,
                                                              String organizationRole,
                                                              String initiativeId,
                                                              ReportRequest request) {

        RewardBatchAssignee operatorLevel = resolveOperatorLevel(organizationRole);

        return merchantRestClient.getMerchantDetail(merchantId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        MERCHANT_NOT_FOUND,
                        ERROR_MESSAGE_MERCHANT_NOT_FOUND.formatted(merchantId, initiativeId) )))
                .flatMap(merchant -> {

                    String formattedDate = LocalDateTime.now().format(FILE_NAME_FORMAT);
                    String fileName = String.format("Report_%s", formattedDate);

                    Report reportEntity = Report.builder()
                            .initiativeId(initiativeId)
                            .reportStatus(ReportStatus.INSERTED)
                            .startPeriod(request.getStartPeriod())
                            .endPeriod(request.getEndPeriod())
                            .merchantId(merchantId)
                            .businessName(merchant.getBusinessName())
                            .requestDate(LocalDateTime.now())
                            .operatorLevel(operatorLevel)
                            .fileName(fileName)
                            .reportType(request.getReportType())
                            .build();

                    return reportRepository.save(reportEntity);
                })
                .map(reportMapper::toDTO)
                .doOnSuccess(saved -> log.info("[GENERATE_REPORT] Saved report {} for merchant {}",
                        saved.getFileName(), Utilities.sanitizeString(merchantId)));

    }

    private RewardBatchAssignee resolveOperatorLevel(String organizationRole) {
        if ("operator1".equals(organizationRole)) return RewardBatchAssignee.L1;
        if ("operator2".equals(organizationRole)) return RewardBatchAssignee.L2;
        if ("operator3".equals(organizationRole)) return RewardBatchAssignee.L3;
        return null;
    }


    @Override
    public Mono<ReportDTO> patchReport(String initiativeId,
                                       String reportId,
                                       PatchReportRequest request) {

        return reportRepository.findByIdAndInitiativeId(reportId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REPORT_NOT_FOUND,
                        ERROR_MESSAGE_REPORT_NOT_FOUND.formatted(reportId, initiativeId)
                )))
                .flatMap(report -> {
                    if (request.getReportStatus() != null) {
                        report.setReportStatus(request.getReportStatus());
                    }

                    return reportRepository.save(report);
                })
                .map(reportMapper::toDTO);
    }


}
