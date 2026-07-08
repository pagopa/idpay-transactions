package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportSpecificRepositoryImplTest {

    @Mock
    private ReactiveMongoTemplate mongoTemplate;

    @InjectMocks
    private ReportSpecificRepositoryImpl repository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void findReportsCombined_shouldReturnReports() {
        Report report = new Report();

        when(mongoTemplate.find(any(Query.class), eq(Report.class)))
                .thenReturn(Flux.just(report));

        Pageable pageable = PageRequest.of(0, 10);

        Flux<Report> result = repository.findReportsCombined(
                "merchantId",
                null,
                "initiativeId",
                ReportType.USER_DETAILS,
                pageable
        );

        StepVerifier.create(result)
                .expectNext(report)
                .verifyComplete();

        verify(mongoTemplate).find(any(Query.class), eq(Report.class));
    }

    @Test
    void countReportsCombined_shouldReturnCount() {
        when(mongoTemplate.count(any(Query.class), eq(Report.class)))
                .thenReturn(Mono.just(5L));

        Mono<Long> result = repository.countReportsCombined(
                "merchantId",
                null,
                "initiativeId",
                ReportType.USER_DETAILS
        );

        StepVerifier.create(result)
                .expectNext(5L)
                .verifyComplete();

        verify(mongoTemplate).count(any(Query.class), eq(Report.class));
    }

    @Test
    void findReportsCombined_withOrganizationRole_shouldReturnReports() {
        Report report = new Report();

        when(mongoTemplate.find(any(Query.class), eq(Report.class)))
                .thenReturn(Flux.just(report));

        Pageable pageable = PageRequest.of(0, 10);

        Flux<Report> result = repository.findReportsCombined(
                null,
                "ADMIN",
                "initiativeId",
                ReportType.USER_DETAILS,
                pageable
        );

        StepVerifier.create(result)
                .expectNext(report)
                .verifyComplete();
    }

    @Test
    void findReportsCombined_withoutOptionalFields_shouldWork() {
        Report report = new Report();

        when(mongoTemplate.find(any(Query.class), eq(Report.class)))
                .thenReturn(Flux.just(report));

        Pageable pageable = PageRequest.of(0, 10);

        Flux<Report> result = repository.findReportsCombined(
                null,
                null,
                null,
                null,
                pageable
        );

        StepVerifier.create(result)
                .expectNext(report)
                .verifyComplete();
    }

    @Test
    void countReportsCombined_withAllFilters_shouldReturnCount() {
        when(mongoTemplate.count(any(Query.class), eq(Report.class)))
                .thenReturn(Mono.just(3L));

        Mono<Long> result = repository.countReportsCombined(
                "merchantId",
                "ADMIN",
                "initiativeId",
                ReportType.USER_DETAILS
        );

        StepVerifier.create(result)
                .expectNext(3L)
                .verifyComplete();
    }
}