package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ReportSpecificRepositoryImpl implements ReportSpecificRepository {

    private final ReactiveMongoTemplate mongoTemplate;

    public ReportSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Flux<Report> findReportsCombined(String merchantId, String organizationRole, String initiativeId, ReportType reportType, Pageable pageable) {

        Criteria criteria = buildCombinedCriteria(merchantId, organizationRole, initiativeId, reportType);
        Query query = Query.query(criteria).with(pageable);
        return mongoTemplate.find(query, Report.class);
    }

    @Override
    public Mono<Long> countReportsCombined(String merchantId, String organizationRole, String initiativeId, ReportType reportType) {

        Criteria criteria = buildCombinedCriteria(merchantId, organizationRole, initiativeId, reportType);
        return mongoTemplate.count(Query.query(criteria), Report.class);
    }

    private Criteria buildCombinedCriteria(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType
    ) {

        List<Criteria> subCriteria = new ArrayList<>();

        if (initiativeId != null && !initiativeId.isBlank()) {
            subCriteria.add(
                    Criteria.where(Report.Fields.initiativeId).is(initiativeId)
            );
        }

        if (reportType != null) {
            subCriteria.add(
                    Criteria.where(Report.Fields.reportType).is(reportType)
            );
        }

        if (merchantId != null && !merchantId.isBlank()) {
            subCriteria.add(
                    Criteria.where(Report.Fields.merchantId).is(merchantId)
            );

            subCriteria.add(
                    Criteria.where(Report.Fields.operatorLevel).is(null)
            );
        }

        if (organizationRole != null && !organizationRole.isBlank()) {
            subCriteria.add(
                    Criteria.where(Report.Fields.operatorLevel).ne(null)
            );
        }

        return new Criteria().andOperator(subCriteria.toArray(new Criteria[0]));
    }

}
