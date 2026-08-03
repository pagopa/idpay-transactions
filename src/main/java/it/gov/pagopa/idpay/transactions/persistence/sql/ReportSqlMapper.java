package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.ReportsRecord;
import org.springframework.stereotype.Component;

@Component
public class ReportSqlMapper {

    ReportEntity toEntity(Report report) {
        return new ReportEntity(
                report.getId(),
                report.getInitiativeId(),
                report.getReportStatus().name(),
                report.getStartPeriod(),
                report.getEndPeriod(),
                report.getMerchantId(),
                report.getBusinessName(),
                report.getRequestDate(),
                report.getElaborationDate(),
                enumName(report.getOperatorLevel()),
                report.getFileName(),
                report.getReportType().name()
        );
    }

    Report fromEntity(ReportEntity entity) {
        return Report.builder()
                .id(entity.id())
                .initiativeId(entity.initiativeId())
                .reportStatus(ReportStatus.valueOf(entity.reportStatus()))
                .startPeriod(entity.startPeriod())
                .endPeriod(entity.endPeriod())
                .merchantId(entity.merchantId())
                .businessName(entity.businessName())
                .requestDate(entity.requestDate())
                .elaborationDate(entity.elaborationDate())
                .operatorLevel(enumValue(entity.operatorLevel()))
                .fileName(entity.fileName())
                .reportType(ReportType.valueOf(entity.reportType()))
                .build();
    }

    Report fromRecord(ReportsRecord reportsRecord) {
        return fromEntity(new ReportEntity(
                reportsRecord.getId(),
                reportsRecord.getInitiativeId(),
                reportsRecord.getReportStatus(),
                reportsRecord.getStartPeriod(),
                reportsRecord.getEndPeriod(),
                reportsRecord.getMerchantId(),
                reportsRecord.getBusinessName(),
                reportsRecord.getRequestDate(),
                reportsRecord.getElaborationDate(),
                reportsRecord.getOperatorLevel(),
                reportsRecord.getFileName(),
                reportsRecord.getReportType()
        ));
    }

    private static String enumName(RewardBatchAssignee value) {
        return value == null ? null : value.name();
    }

    private static RewardBatchAssignee enumValue(String value) {
        return value == null ? null : RewardBatchAssignee.valueOf(value);
    }
}
