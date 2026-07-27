package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardBatchAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchAdapter(
                databaseClient(),
                transactionalOperator(),
                new RewardBatchSqlMapper(JsonMapper.builder().build())
        );
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient()
                .sql("DELETE FROM reward_transactions")
                .fetch()
                .rowsUpdated()
                .then(databaseClient()
                        .sql("DELETE FROM reward_batches")
                        .fetch()
                        .rowsUpdated())
                .block();
    }

    @Test
    void shouldCreateOrReadExactlyOneBatchWhenGroupingInsertsRace() {
        RewardBatch first = batch("batch-first");
        RewardBatch second = batch("batch-second");

        StepVerifier.create(Flux.merge(
                        adapter.createOrRead(first),
                        adapter.createOrRead(second)
                ).collectList())
                .assertNext(created -> {
                    assertEquals(2, created.size());
                    assertTrue(created.stream()
                            .map(RewardBatch::getId)
                            .allMatch(created.getFirst().getId()::equals));
                })
                .verifyComplete();

        StepVerifier.create(adapter.findByGrouping(
                        first.getInitiativeId(),
                        first.getMerchantId(),
                        first.getPosType(),
                        first.getMonth()
                ))
                .expectNextMatches(found -> found.getId().equals(first.getId()) || found.getId().equals(second.getId()))
                .verifyComplete();
    }

    @Test
    void shouldReadBatchOnlyWithinItsRequestedIdentityScope() {
        RewardBatch batch = batch("batch-identity");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(adapter.findByIdAndInitiativeId(batch.getId(), batch.getInitiativeId()))
                        .zipWith(adapter.findByMerchantInitiativeAndId(
                                batch.getMerchantId(),
                                batch.getInitiativeId(),
                                batch.getId()
                        )))
                .assertNext(batches -> assertEquals(batches.getT1(), batches.getT2()))
                .verifyComplete();

        StepVerifier.create(adapter.findByIdAndInitiativeId(batch.getId(), "other-initiative"))
                .verifyComplete();

        StepVerifier.create(adapter.findByMerchantInitiativeAndId(
                        "other-merchant",
                        batch.getInitiativeId(),
                        batch.getId()
                ))
                .verifyComplete();
    }

    @Test
    void shouldUpdateStatusAndMetadataWithoutChangingBatchIdentity() {
        RewardBatch batch = batch("batch-metadata");
        DeliveryOutcomeDTO deliveryOutcome = DeliveryOutcomeDTO.builder()
                .idRichiesta("delivery-request")
                .succeded(true)
                .message("accepted")
                .code(200)
                .build();

        StepVerifier.create(adapter.createOrRead(batch)
                        .flatMap(created -> adapter.updateStatus(
                                created.getId(),
                                created.getInitiativeId(),
                                RewardBatchStatus.SENT
                        ))
                        .flatMap(updated -> {
                            updated.setFilename("approved.csv");
                            updated.setReportPath("initiative/initiative-1/batch/approved.csv");
                            updated.setMerchantSendDate(LocalDateTime.of(2026, 7, 1, 10, 30));
                            updated.setDeliveryOutcome(deliveryOutcome);
                            updated.setAssigneeLevel(RewardBatchAssignee.L2);
                            return adapter.updateMetadata(updated);
                        }))
                .assertNext(updated -> {
                    assertEquals(batch.getId(), updated.getId());
                    assertEquals(batch.getInitiativeId(), updated.getInitiativeId());
                    assertEquals(RewardBatchStatus.SENT, updated.getStatus());
                    assertEquals("approved.csv", updated.getFilename());
                    assertEquals("initiative/initiative-1/batch/approved.csv", updated.getReportPath());
                    assertEquals(deliveryOutcome.getIdRichiesta(), updated.getDeliveryOutcome().getIdRichiesta());
                    assertEquals(deliveryOutcome.isSucceded(), updated.getDeliveryOutcome().isSucceded());
                    assertEquals(RewardBatchAssignee.L2, updated.getAssigneeLevel());
                })
                .verifyComplete();
    }

    private static RewardBatch batch(String id) {
        return RewardBatch.builder()
                .id(id)
                .initiativeId("initiative-1")
                .merchantId("merchant-1")
                .businessName("Merchant")
                .month("2026-07")
                .posType(PosType.PHYSICAL)
                .status(RewardBatchStatus.CREATED)
                .partial(false)
                .name("Luglio 2026")
                .startDate(LocalDateTime.of(2026, 7, 1, 0, 0))
                .endDate(LocalDateTime.of(2026, 7, 31, 23, 59, 59))
                .creationDate(LocalDateTime.of(2026, 7, 1, 0, 0))
                .updateDate(LocalDateTime.of(2026, 7, 1, 0, 0))
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();
    }
}
