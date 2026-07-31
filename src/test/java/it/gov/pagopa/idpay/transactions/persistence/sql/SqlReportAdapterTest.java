package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDateTime;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class SqlReportAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlReportAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlReportAdapter(
                transactionalOperator(),
                new R2dbcRepositoryFactory(r2dbcEntityTemplate()).getRepository(ReportSqlRepository.class),
                r2dbcEntityTemplate(),
                DSL.using(new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES),
                new ReportSqlMapper()
        );
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient().sql("DELETE FROM reports").fetch().rowsUpdated().block();
    }

    @Test
    void insertsGeneratedIdReportBeforeItCanBeUpdated() {
        Report report = report(null, "initiative", "merchant", null, "first", 1);

        StepVerifier.create(adapter.save(report)
                        .flatMap(saved -> adapter.findByIdAndInitiativeId(
                                saved.getId(), "initiative")))
                .assertNext(loaded -> {
                    assertNotNull(report.getId());
                    assertEquals(report.getId(), loaded.getId());
                    assertEquals("first", loaded.getFileName());
                })
                .verifyComplete();

        report.setFileName("updated");
        StepVerifier.create(adapter.save(report)
                        .flatMap(updated -> adapter.findByIdAndInitiativeId(
                                updated.getId(), "initiative")))
                .assertNext(loaded -> assertEquals("updated", loaded.getFileName()))
                .verifyComplete();

        StepVerifier.create(adapter.findByIdAndInitiativeId(report.getId(), "other-initiative"))
                .verifyComplete();
    }

    @Test
    void appliesMerchantAndOperatorScopesWithStablePaginationAndCounts() {
        Report merchantFirst = report("merchant-first", "initiative", "merchant", null, "first", 1);
        Report merchantSecond = report("merchant-second", "initiative", "merchant", null, "second", 2);
        Report otherMerchant = report("other-merchant", "initiative", "other", null, "other", 3);
        Report operator = report("operator", "initiative", null, RewardBatchAssignee.L1, "operator", 4);

        StepVerifier.create(Flux.concat(
                        adapter.save(merchantFirst),
                        adapter.save(merchantSecond),
                        adapter.save(otherMerchant),
                        adapter.save(operator))
                .thenMany(adapter.findReports(
                        "merchant", null, "initiative", ReportType.MERCHANT_TRANSACTIONS,
                        PageRequest.of(0, 1, Sort.by(Sort.Direction.ASC, "requestDate")))))
                .assertNext(report -> assertEquals("merchant-first", report.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.countReports(
                        "merchant", null, "initiative", ReportType.MERCHANT_TRANSACTIONS))
                .expectNext(2L)
                .verifyComplete();
        StepVerifier.create(adapter.findReports(
                        null, "operator1", "initiative", ReportType.MERCHANT_TRANSACTIONS,
                        PageRequest.of(0, 10)))
                .assertNext(report -> assertEquals("operator", report.getId()))
                .verifyComplete();
        StepVerifier.create(adapter.countReports(
                        null, "operator1", "initiative", ReportType.MERCHANT_TRANSACTIONS))
                .expectNext(1L)
                .verifyComplete();
        StepVerifier.create(adapter.findByIdAndInitiativeIdAndMerchantId(
                        "merchant-first", "initiative", "other"))
                .verifyComplete();
    }

    private static Report report(
            String id,
            String initiativeId,
            String merchantId,
            RewardBatchAssignee operatorLevel,
            String filename,
            int hour
    ) {
        LocalDateTime time = LocalDateTime.of(2026, 1, 1, hour, 0);
        return Report.builder()
                .id(id)
                .initiativeId(initiativeId)
                .reportStatus(ReportStatus.INSERTED)
                .startPeriod(time.minusDays(1))
                .endPeriod(time)
                .merchantId(merchantId)
                .operatorLevel(operatorLevel)
                .requestDate(time)
                .fileName(filename)
                .reportType(ReportType.MERCHANT_TRANSACTIONS)
                .build();
    }
}
