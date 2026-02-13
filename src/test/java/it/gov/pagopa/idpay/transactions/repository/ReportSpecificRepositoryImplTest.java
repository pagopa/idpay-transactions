package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.model.Report;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DirtiesContext
@MongoTest
class ReportSpecificRepositoryImplTest {

    @Autowired
    private ReportRepository reportRepository;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    private ReportSpecificRepositoryImpl reportSpecificRepository;

    private static final String INITIATIVE_ID = "INITIATIVE1";
    private static final String MERCHANT_ID = "MERCHANT1";

    @BeforeEach
    void setup() {
        reportSpecificRepository = new ReportSpecificRepositoryImpl(mongoTemplate);
        reportRepository.deleteAll().block();
    }

    @AfterEach
    void cleanup() {
        reportRepository.deleteAll().block();
    }

    @Test
    void findReportsCombined_shouldReturnReports_whenCriteriaMatch() {
        Report report = Report.builder()
                .id("R1")
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .businessName("Business1")
                .reportStatus(ReportStatus.INSERTED)
                .operatorLevel(null)
                .fileName("file1.csv")
                .startPeriod(LocalDateTime.of(2026, 2, 1, 0, 0))
                .endPeriod(LocalDateTime.of(2026, 2, 28, 23, 59))
                .requestDate(LocalDateTime.now())
                .elaborationDate(LocalDateTime.now())
                .build();

        reportRepository.save(report).block();

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(reportSpecificRepository.findReportsCombined(MERCHANT_ID, null, INITIATIVE_ID, pageable))
                .assertNext(r -> {
                    assertEquals("R1", r.getId());
                    assertEquals(MERCHANT_ID, r.getMerchantId());
                    assertEquals(INITIATIVE_ID, r.getInitiativeId());
                })
                .verifyComplete();
    }

    @Test
    void findReportsCombined_shouldReturnEmpty_whenNoMatch() {
        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(reportSpecificRepository.findReportsCombined(MERCHANT_ID, null, INITIATIVE_ID, pageable))
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void countReportsCombined_shouldReturnCorrectCount() {
        Report report1 = Report.builder().id("R1").merchantId(MERCHANT_ID).initiativeId(INITIATIVE_ID).build();
        Report report2 = Report.builder().id("R2").merchantId(MERCHANT_ID).initiativeId(INITIATIVE_ID).build();

        reportRepository.saveAll(List.of(report1, report2)).collectList().block();

        StepVerifier.create(reportSpecificRepository.countReportsCombined(MERCHANT_ID, null, INITIATIVE_ID))
                .assertNext(count -> assertEquals(2L, count))
                .verifyComplete();
    }

    @Test
    void countReportsCombined_shouldReturnZero_whenNoMatch() {
        StepVerifier.create(reportSpecificRepository.countReportsCombined(MERCHANT_ID, null, INITIATIVE_ID))
                .assertNext(count -> assertEquals(0L, count))
                .verifyComplete();
    }

    @Test
    void findReportsCombined_shouldFilterByRewardBatchAssignee() {

        Report report1 = Report.builder()
                .id("R1")
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .operatorLevel(null)
                .build();

        Report report2 = Report.builder()
                .id("R2")
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .operatorLevel(RewardBatchAssignee.L1)
                .build();

        reportRepository.saveAll(List.of(report1, report2))
                .collectList()
                .block();

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(
                        reportSpecificRepository.findReportsCombined(
                                MERCHANT_ID,
                                null,
                                INITIATIVE_ID,
                                pageable
                        )
                )
                .expectNextMatches(r -> r.getId().equals("R1"))
                .verifyComplete();
    }

    @Test
    void findReportsCombined_shouldFilterByOrganizationRole() {

        Report report1 = Report.builder()
                .id("R1")
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .operatorLevel(null)
                .build();

        Report report2 = Report.builder()
                .id("R2")
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .operatorLevel(RewardBatchAssignee.L1)
                .build();

        reportRepository.saveAll(List.of(report1, report2))
                .collectList()
                .block();

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(
                        reportSpecificRepository.findReportsCombined(
                                null,
                                "Operator1",
                                INITIATIVE_ID,
                                pageable
                        )
                )
                .expectNextMatches(r -> r.getId().equals("R2"))
                .verifyComplete();
    }

    @Test
    void findReportsCombined_shouldWork_whenMerchantIdIsNull() {

        Report report = Report.builder()
                .id("R1")
                .initiativeId(INITIATIVE_ID)
                .operatorLevel(RewardBatchAssignee.L1)
                .build();

        reportRepository.save(report).block();

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(
                        reportSpecificRepository.findReportsCombined(
                                null,
                                "Operator1",
                                INITIATIVE_ID,
                                pageable
                        )
                )
                .expectNextMatches(r -> r.getId().equals("R1"))
                .verifyComplete();
    }

}
