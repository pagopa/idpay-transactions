package it.gov.pagopa.idpay.transactions.persistence.sql;

import java.time.LocalDateTime;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("reports")
public record ReportEntity(
        @Id String id,
        @Column("initiative_id") String initiativeId,
        @Column("report_status") String reportStatus,
        @Column("start_period") LocalDateTime startPeriod,
        @Column("end_period") LocalDateTime endPeriod,
        @Column("merchant_id") String merchantId,
        @Column("business_name") String businessName,
        @Column("request_date") LocalDateTime requestDate,
        @Column("elaboration_date") LocalDateTime elaborationDate,
        @Column("operator_level") String operatorLevel,
        @Column("file_name") String fileName,
        @Column("report_type") String reportType
) {
}
