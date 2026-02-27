package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.DownloadReportResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.data.factory.DataFactoryService;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.dto.report.Report2RunDto;
import it.gov.pagopa.idpay.transactions.dto.report.ReportGenerateForce;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.exception.AzureConnectingErrorException;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.repository.ReportRepository;
import it.gov.pagopa.idpay.transactions.storage.ReportBlobService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.*;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Slf4j
@Service
public class ReportServiceImpl implements ReportService {

    private final ReportRepository reportRepository;

    private final MerchantRestClient merchantRestClient;

    private final ReportMapper reportMapper;

    private final ReportBlobService reportBlobService;

    private final DataFactoryService dataFactoryService;

    private final long periodLengthTransactionsReport;

    public ReportServiceImpl(
            @Value("${app.period-length-transactions-report}") long periodLengthTransactionsReport,
            ReportRepository reportRepository,
            MerchantRestClient merchantRestClient,
            ReportMapper reportMapper,
            ReportBlobService reportBlobService,
            DataFactoryService dataFactoryService) {
        this.periodLengthTransactionsReport = periodLengthTransactionsReport;
        this.reportRepository = reportRepository;
        this.merchantRestClient = merchantRestClient;
        this.reportMapper = reportMapper;
        this.reportBlobService = reportBlobService;
        this.dataFactoryService = dataFactoryService;
    }

    static final List<String> ALLOWED_ROLES = List.of(
            "operator1", "operator2", "operator3"
    );
    private static final DateTimeFormatter FILE_NAME_FORMAT = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss");

    private static final String REPORT_TRANSACTIONS_PATH_STORAGE_FORMAT = "initiative/%s/merchant/%s/report/%s";

    @Override
    public Mono<Page<Report>> getReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType,
            Pageable pageable
    ) {

        //For backward compatibility for the merchant portal
        if (merchantId != null && !merchantId.isBlank()) {
            reportType = ReportType.MERCHANT_TRANSACTIONS;
        }

        if (reportType == null) {
            log.warn("[GET_REPORTS] Missing mandatory filters: reportType null");
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    REPORT_TYPE_REQUIRED,
                    ERROR_MESSAGE_REPORT_TYPE_REQUIRED
            );
        }

        return switch (reportType) {
            case MERCHANT_TRANSACTIONS -> getTransactionsReports(
                        merchantId,
                        organizationRole,
                        initiativeId,
                        pageable
                );

            case USER_DETAILS -> getUserDetailsReports(
                        organizationRole,
                        initiativeId,
                        pageable
                );

        };
    }

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

        if (merchantId != null) {
            log.info("[GET_TRANSACTIONS_REPORTS] Fetching reports for initiative: {}, merchant: {}", Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(merchantId));
        } else {
            log.info("[GET_TRANSACTIONS_REPORTS] Fetching reports for initiative: {}, role: {}", Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(organizationRole));
        }

        Pageable sortedPageable = PageRequest.of( pageable.getPageNumber(),
                pageable.getPageSize(), Sort.by(Sort.Direction.DESC, "requestDate"));

        return reportRepository.findReportsCombined(
                        merchantId,
                        organizationRole,
                        initiativeId,
                        ReportType.MERCHANT_TRANSACTIONS,
                        sortedPageable

                )
                .collectList()
                .zipWith(reportRepository.countReportsCombined(
                        merchantId,
                        organizationRole,
                        initiativeId,
                        ReportType.MERCHANT_TRANSACTIONS
                ))
                .flatMap(tuple -> Mono.just(new PageImpl<>(tuple.getT1(), sortedPageable, tuple.getT2())));
    }

    @Override
    public Mono<Page<Report>> getUserDetailsReports(
            String organizationRole,
            String initiativeId,
            Pageable pageable
    ) {

        if (organizationRole == null || organizationRole.isBlank()) {
            log.warn("[GET_USER_DETAILS_REPORTS] Missing mandatory filter: organizationRole is null or blank");

            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY,
                    ERROR_MESSAGE_MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY
            );
        }

        if (ALLOWED_ROLES.stream().noneMatch(role -> role.equalsIgnoreCase(organizationRole))) {

            log.warn("[GET_USER_DETAILS_REPORTS] Invalid organizationRole: {}", Utilities.sanitizeString(organizationRole));
            throw new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    INVALID_ORGANIZATION_ROLE,
                    ERROR_MESSAGE_INVALID_ORGANIZATION_ROLE
            );
        }

        log.info("[GET_USER_DETAILS_REPORTS] Fetching USER_DETAILS reports for initiative: {}, role: {}", Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(organizationRole));
        Pageable sortedPageable = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), Sort.by(Sort.Direction.DESC, "requestDate"));

        return reportRepository.findReportsCombined(
                        null,
                        organizationRole,
                        initiativeId,
                        ReportType.USER_DETAILS,
                        sortedPageable
                )
                .collectList()
                .zipWith(reportRepository.countReportsCombined(
                        null,
                        organizationRole,
                        initiativeId,
                        ReportType.USER_DETAILS
                ))
                .map(tuple -> new PageImpl<>(
                        tuple.getT1(),
                        sortedPageable,
                        tuple.getT2()
                ));
    }

    @Override
    public Mono<ReportDTO> generateReport(String merchantId,
                                   String organizationRole,
                                   String initiativeId,
                                   ReportRequest request){

        if (ReportType.MERCHANT_TRANSACTIONS.equals(request.getReportType())) {
            if(merchantId == null){
                return Mono.error(new ClientExceptionWithBody(
                        HttpStatus.BAD_REQUEST,
                        MERCHANT_ID_REQUIRED,
                        ERROR_MESSAGE_MERCHANT_ID_MANDATORY));
            }
            log.info("[GENERATE_MERCHANT_TRANSACTIONS_REPORT] Requested report with MerchantId = {}, startPeriod = {}, endPeriod = {}",
                    Utilities.sanitizeString(merchantId),
                    request.getStartPeriod(),
                    request.getEndPeriod());
            return generateMerchantTransactionsReport(merchantId, organizationRole, initiativeId, request);
    }
        if (ReportType.USER_DETAILS.equals(request.getReportType())) {
            log.info("[GENERATE_USER_DETAILS_REPORT] Requested report with startPeriod = {}, endPeriod = {}",
                    request.getStartPeriod(),
                    request.getEndPeriod());
            return generateUserDetailsReport(organizationRole, initiativeId, request);
        }
        return Mono.empty();
}


    public Mono<ReportDTO> generateMerchantTransactionsReport(String merchantId,
                                                              String organizationRole,
                                                              String initiativeId,
                                                              ReportRequest request) {

        if(!(request.getEndPeriod().isBefore(LocalDate.now().atStartOfDay())
            && request.getStartPeriod().isBefore(request.getEndPeriod()))){
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    INVALID_PERIOD,
                    ERROR_MESSAGE_INVALID_PERIOD));
        }

        if(ChronoUnit.DAYS.between(request.getStartPeriod(), request.getEndPeriod()) > periodLengthTransactionsReport){
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    INVALID_LENGTH_PERIOD,
                    ERROR_MESSAGE_INVALID_LENGTH_PERIOD.formatted(periodLengthTransactionsReport)));
        }
        RewardBatchAssignee operatorLevel = resolveOperatorLevel(organizationRole);

        return merchantRestClient.getMerchantDetail(merchantId, initiativeId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        MERCHANT_NOT_FOUND,
                        ERROR_MESSAGE_MERCHANT_NOT_FOUND.formatted(merchantId, initiativeId) )))
                .flatMap(merchant -> {

                    String formattedDate = LocalDateTime.now().format(FILE_NAME_FORMAT);
                    String fileName = String.format("Report_%s.csv", formattedDate);

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
                .flatMap(report ->
                        triggerTransactionReportPipeline(report)
                                .thenReturn(report)
                                .onErrorResume(AzureConnectingErrorException.class, ex -> {
                                    report.setReportStatus(ReportStatus.FAILED);
                                    return reportRepository.save(report);
                                })
                )
                .map(reportMapper::toDTO)
                .doOnSuccess(saved -> log.info("[GENERATE_MERCHANT_TRANSACTIONS_REPORT] Saved report {} for merchant {}",
                        saved.getFileName(), Utilities.sanitizeString(merchantId)));

    }

    public Mono<ReportDTO> generateUserDetailsReport(String organizationRole,
                                                     String initiativeId,
                                                     ReportRequest request) {

        if (!(request.getEndPeriod().isBefore(LocalDate.now().atStartOfDay())
                && request.getStartPeriod().isBefore(request.getEndPeriod()))) {
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    INVALID_PERIOD,
                    ERROR_MESSAGE_INVALID_PERIOD));
        }

        RewardBatchAssignee operatorLevel = resolveOperatorLevel(organizationRole);
        String formattedDate = LocalDateTime.now().format(FILE_NAME_FORMAT);
        String fileName = String.format("Report_%s.csv", formattedDate);

        Report reportEntity = Report.builder()
                .initiativeId(initiativeId)
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(request.getStartPeriod())
                .endPeriod(request.getEndPeriod())
                .requestDate(LocalDateTime.now())
                .operatorLevel(operatorLevel)
                .fileName(fileName)
                .reportType(request.getReportType())
                .build();

        return reportRepository.save(reportEntity)
                .flatMap(report ->
                        triggerUserDetailsReportPipeline(report)
                                .thenReturn(report)
                                .onErrorResume(AzureConnectingErrorException.class, ex -> {
                                    log.error("[GENERATE_USER_DETAILS_REPORT] Error triggering pipeline", ex);
                                    report.setReportStatus(ReportStatus.FAILED);
                                    return reportRepository.save(report);
                                })
                )
                .map(reportMapper::toDTO)
                .doOnSuccess(saved -> log.info("[GENERATE_USER_DETAILS_REPORT] Saved report {} for initiative {}",
                        saved.getFileName(), Utilities.sanitizeString(initiativeId)));
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
                    if(ReportStatus.GENERATED.equals(request.getReportStatus())){
                        report.setElaborationDate(LocalDateTime.now());
                    }

                    return reportRepository.save(report);
                })
                .map(reportMapper::toDTO);
    }


    @Override
    public Mono<List<Report2RunDto>> forceGenerateReports(ReportGenerateForce reportGenerateForce) {
        log.info("[RUN_GENERATE_REPORT] Request generate report {}",  Utilities.sanitizeString(String.valueOf(reportGenerateForce.getReportsId())));
        return reportRepository.findAllById(reportGenerateForce.getReportsId())
                .flatMap(this::triggerTransactionReportPipeline)
                .collectList();
    }

    private Mono<Report2RunDto> triggerTransactionReportPipeline(Report report) {
        return dataFactoryService.triggerTransactionReportPipeline(report)
                .map(runId ->
                        Report2RunDto.builder()
                                .reportId(report.getId())
                                .runId(runId).build());
    }

    private Mono<Report2RunDto> triggerUserDetailsReportPipeline(Report report) {
        return dataFactoryService.triggerUserDetailsReportPipeline(report)
                .map(runId ->
                        Report2RunDto.builder()
                                .reportId(report.getId())
                                .runId(runId).build());
    }
    @Override
    public Mono<DownloadReportResponseDTO> downloadTransactionsReport(
            String merchantId,
            String organizationRole,
            String initiativeId,
            String reportId
    ) {

        if ((merchantId == null || merchantId.isBlank()) &&
                (organizationRole == null || organizationRole.isBlank())) {

            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY,
                    ERROR_MESSAGE_MERCHANT_ID_OR_ORGANIZATION_ROLE_ARE_MANDATORY
            ));
        }

        if (merchantId != null && organizationRole != null) {
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    MERCHANT_ID_AND_ORGANIZATION_ROLE_CANNOT_COEXIST,
                    ERROR_MESSAGE_MERCHANT_ID_AND_ORGANIZATION_ROLE_CANNOT_COEXIST
            ));
        }

        if (organizationRole != null &&
                ALLOWED_ROLES.stream().noneMatch(role -> role.equalsIgnoreCase(organizationRole))) {

            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    INVALID_ORGANIZATION_ROLE,
                    ERROR_MESSAGE_INVALID_ORGANIZATION_ROLE
            ));
        }

        Mono<Report> query = merchantId == null
                ? reportRepository.findByIdAndInitiativeId(reportId, initiativeId)
                : reportRepository.findByIdAndInitiativeIdAndMerchantId(reportId, initiativeId, merchantId);

        return query
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        REPORT_NOT_FOUND,
                        ERROR_MESSAGE_REPORT_NOT_FOUND.formatted(reportId, initiativeId)
                )))
                .map(report -> {

                    if (!ReportStatus.GENERATED.equals(report.getReportStatus())) {
                        throw new ClientExceptionWithBody(
                                HttpStatus.BAD_REQUEST,
                                REPORT_NOT_GENERATED,
                                ERROR_MESSAGE_REPORT_NOT_GENERATED.formatted(reportId)
                        );
                    }

                    String filename = report.getFileName();
                    if (filename == null || filename.isBlank()) {
                        throw new ClientExceptionWithBody(
                                HttpStatus.INTERNAL_SERVER_ERROR,
                                REPORT_MISSING_FILENAME,
                                ERROR_MESSAGE_REPORT_MISSING_FILENAME.formatted(reportId)
                        );
                    }

                    String blobPath = String.format(
                            REPORT_TRANSACTIONS_PATH_STORAGE_FORMAT,
                            initiativeId,
                            report.getMerchantId(),
                            filename
                    );

                    return DownloadReportResponseDTO.builder()
                            .reportUrl(reportBlobService.getFileSignedUrl(blobPath))
                            .build();
                });
    }

}
