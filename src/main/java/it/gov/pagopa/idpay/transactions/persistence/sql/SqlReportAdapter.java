package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.Reports.REPORTS;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.trueCondition;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.persistence.port.ReportPersistencePort;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SortField;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SqlReportAdapter implements ReportPersistencePort {

    private static final Map<String, Field<?>> SORTABLE_FIELDS = Map.ofEntries(
            Map.entry("id", REPORTS.ID),
            Map.entry("initiativeId", REPORTS.INITIATIVE_ID),
            Map.entry("reportStatus", REPORTS.REPORT_STATUS),
            Map.entry("startPeriod", REPORTS.START_PERIOD),
            Map.entry("endPeriod", REPORTS.END_PERIOD),
            Map.entry("merchantId", REPORTS.MERCHANT_ID),
            Map.entry("businessName", REPORTS.BUSINESS_NAME),
            Map.entry("requestDate", REPORTS.REQUEST_DATE),
            Map.entry("elaborationDate", REPORTS.ELABORATION_DATE),
            Map.entry("operatorLevel", REPORTS.OPERATOR_LEVEL),
            Map.entry("fileName", REPORTS.FILE_NAME),
            Map.entry("reportType", REPORTS.REPORT_TYPE)
    );

    private final TransactionalOperator transactionalOperator;
    private final ReportSqlRepository repository;
    private final R2dbcEntityTemplate r2dbcEntityTemplate;
    private final DSLContext dslContext;
    private final ReportSqlMapper mapper;

    @Override
    public Flux<Report> findReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType,
            Pageable pageable
    ) {
        Condition condition = combinedCondition(
                merchantId,
                organizationRole,
                initiativeId,
                reportType
        );
        List<? extends SortField<?>> sortFields = sortFields(pageable == null ? Sort.unsorted() : pageable.getSort());
        if (pageable == null || pageable.isUnpaged()) {
            return Flux.from(dslContext.selectFrom(REPORTS)
                            .where(condition)
                            .orderBy(sortFields))
                    .map(mapper::fromRecord);
        }
        return Flux.from(dslContext.selectFrom(REPORTS)
                        .where(condition)
                        .orderBy(sortFields)
                        .limit(pageable.getPageSize())
                        .offset(pageable.getOffset()))
                .map(mapper::fromRecord);
    }

    @Override
    public Mono<Long> countReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType
    ) {
        return Mono.from(dslContext.select(count())
                        .from(REPORTS)
                        .where(combinedCondition(
                                merchantId,
                                organizationRole,
                                initiativeId,
                                reportType
                        )))
                .map(result -> result.value1().longValue());
    }

    @Override
    public Mono<Report> save(Report report) {
        if (report.getId() == null || report.getId().isBlank()) {
            report.setId(UUID.randomUUID().toString());
        }
        ReportEntity entity = mapper.toEntity(report);
        return transactionalOperator.transactional(repository.existsById(entity.id())
                        .flatMap(exists -> Boolean.TRUE.equals(exists)
                                ? repository.save(entity)
                                : r2dbcEntityTemplate.insert(entity)))
                .map(mapper::fromEntity);
    }

    @Override
    public Mono<Report> findByIdAndInitiativeId(String reportId, String initiativeId) {
        return Mono.from(dslContext.selectFrom(REPORTS)
                        .where(REPORTS.ID.eq(reportId)
                                .and(REPORTS.INITIATIVE_ID.eq(initiativeId))))
                .map(mapper::fromRecord);
    }

    @Override
    public Mono<Report> findByIdAndInitiativeIdAndMerchantId(
            String reportId,
            String initiativeId,
            String merchantId
    ) {
        return Mono.from(dslContext.selectFrom(REPORTS)
                        .where(REPORTS.ID.eq(reportId)
                                .and(REPORTS.INITIATIVE_ID.eq(initiativeId))
                                .and(REPORTS.MERCHANT_ID.eq(merchantId))))
                .map(mapper::fromRecord);
    }

    @Override
    public Flux<Report> findAllById(Iterable<String> reportIds) {
        return repository.findAllById(reportIds).map(mapper::fromEntity);
    }

    private static Condition combinedCondition(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType
    ) {
        Condition condition = trueCondition();
        if (notBlank(initiativeId)) {
            condition = condition.and(REPORTS.INITIATIVE_ID.eq(initiativeId));
        }
        if (reportType != null) {
            condition = condition.and(REPORTS.REPORT_TYPE.eq(reportType.name()));
        }
        if (notBlank(merchantId)) {
            condition = condition.and(REPORTS.MERCHANT_ID.eq(merchantId))
                    .and(REPORTS.OPERATOR_LEVEL.isNull());
        }
        if (notBlank(organizationRole)) {
            condition = condition.and(REPORTS.OPERATOR_LEVEL.isNotNull());
        }
        return condition;
    }

    private static List<? extends SortField<?>> sortFields(Sort sort) {
        if (sort == null || sort.isUnsorted()) {
            return List.of(REPORTS.REQUEST_DATE.desc(), REPORTS.ID.asc());
        }
        List<SortField<?>> fields = sort.stream()
                .<SortField<?>>map(order -> sortableField(order.getProperty(), order.getDirection()))
                .toList();
        boolean hasId = sort.stream().anyMatch(order -> order.getProperty().equalsIgnoreCase("id"));
        return hasId ? fields : appendIdSort(fields);
    }

    private static List<? extends SortField<?>> appendIdSort(List<SortField<?>> sortFields) {
        java.util.ArrayList<SortField<?>> result = new java.util.ArrayList<>(sortFields);
        result.add(REPORTS.ID.asc());
        return result;
    }

    private static SortField<?> sortableField(String property, Sort.Direction direction) {
        Field<?> field = SORTABLE_FIELDS.getOrDefault(property, REPORTS.REQUEST_DATE);
        return direction.isAscending() ? field.asc() : field.desc();
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
