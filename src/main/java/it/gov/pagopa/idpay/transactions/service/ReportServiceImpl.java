package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.repository.MerchantRepository;
import it.gov.pagopa.idpay.transactions.repository.ReportRepository;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY;

@Slf4j
@Service
public class ReportServiceImpl implements ReportService {

    private final ReportRepository reportRepository;

    private final MerchantRepository merchantRepository;

    private final ReportMapper reportMapper;

    public ReportServiceImpl(ReportRepository reportRepository, MerchantRepository merchantRepository, ReportMapper reportMapper) {
        this.reportRepository = reportRepository;
        this.merchantRepository = merchantRepository;
        this.reportMapper = reportMapper;
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

    @Override
    public Mono<ReportDTO> generateReport(String merchantId,
                                   String organizationRole,
                                   String initiativeId,
                                   ReportRequest request){
        if (request.getReportType().equals(ReportType.MERCHANT_TRANSACTIONS)) {
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


        return merchantRepository.findById(merchantId)
                .flatMap(merchant -> {

                    String formattedDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("ddMMyyyyHHmmss"));
                    String fileName = String.format("Report_%s", formattedDate);

                    Report reportEntity = Report.builder()
                            .initiativeId(initiativeId)
                            .reportStatus(ReportStatus.INSERTED)
                            .startPeriod(request.getStartPeriod())
                            .endPeriod(request.getEndPeriod())
                            .merchantId(merchantId)
                            .businessName(merchant.getBusinessName())
                            .requestDate(LocalDateTime.now())
                            .operatorLevel(request.getOperatorLevel())
                            .fileName(fileName)
                            .reportType(request.getReportType())
                            .build();

                    return reportRepository.save(reportEntity);
                })
                .map(reportMapper::toDTO)
                .doOnSuccess(saved -> log.info("[GENERATE_REPORT] Saved report {} for merchant {}",
                        saved.getFileName(), Utilities.sanitizeString(merchantId)));

    }

}
