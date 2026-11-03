package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.common.config.JsonConfig;
import it.gov.pagopa.common.kafka.utils.KafkaConstants;
import it.gov.pagopa.common.reactive.kafka.exception.PermanentMessageException;
import it.gov.pagopa.common.reactive.kafka.exception.UncommittableError;
import it.gov.pagopa.common.utils.TestUtils;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.RewardTransactionEvent;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class PersistenceTransactionMediatorImplTest {

    @Mock
    private RewardTransactionService rewardTransactionService;

    @Mock
    private TransactionErrorNotifierService transactionErrorNotifierService;

    @Mock
    private RewardTransactionMapper rewardTransactionMapper;

    private PersistenceTransactionMediatorImpl persistenceTransactionMediator;

    @BeforeEach
    void setUp() {
        persistenceTransactionMediator = new PersistenceTransactionMediatorImpl(
                "appName",
                rewardTransactionService,
                transactionErrorNotifierService,
                rewardTransactionMapper,
                1000,
                new JsonConfig().objectMapper()
        );
    }

    @Test
    void execute() {
        RewardTransactionDTO rtDT1 = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransactionDTO rtDT2 = RewardTransactionDTOFaker.mockInstance(2);

        Message<String> firstMessage = kafkaMessage(TestUtils.jsonSerializer(rtDT1), 0L);
        Message<String> failingMessage = kafkaMessage(TestUtils.jsonSerializer(rtDT2), 1L);
        Flux<Message<String>> messageFlux = Flux.just(firstMessage, failingMessage);

        RewardTransaction rt1 = RewardTransactionFaker.mockInstance(1);
        rt1.setStatus(SyncTrxStatus.AUTHORIZED.name());
        rt1.setInitiatives(List.of("initiative-1"));

        Mockito.when(rewardTransactionMapper.mapFromDTO(Mockito.any(RewardTransactionDTO.class)))
                .thenReturn(rt1)
                .thenThrow(new RuntimeException("boom"));

        Mockito.when(rewardTransactionService.save(Mockito.any(RewardTransactionEvent.class)))
                .thenReturn(Mono.just(rt1));

        StepVerifier.withVirtualTime(() -> {
                    CapturingPersistenceTransactionMediator capturingMediator =
                            capturingMediator(60_000, rewardTransactionMapper);
                    capturingMediator.execute(messageFlux);
                    return capturingMediator.afterCommits();
                })
                .thenRequest(Long.MAX_VALUE)
                .thenAwait(Duration.ofSeconds(20))
                .assertNext(results -> org.junit.jupiter.api.Assertions.assertEquals(List.of(rt1), results))
                .verifyComplete();

        Mockito.verify(rewardTransactionMapper, Mockito.times(5))
                .mapFromDTO(Mockito.any(RewardTransactionDTO.class));
        Mockito.verify(rewardTransactionService, Mockito.times(1))
                .save(Mockito.any(RewardTransactionEvent.class));
        Mockito.verify(transactionErrorNotifierService, Mockito.times(1))
                .notifyTransaction(
                        same(failingMessage),
                        Mockito.anyString(),
                        Mockito.eq(true),
                        Mockito.any(RuntimeException.class)
                );
    }

    @Test
    void executeShouldMapAndSaveInvoicedTransactionWithoutPaymentDeletion() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.INVOICED.name());
        rt.setInitiatives(List.of("initiative-1"));

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(Mockito.any(RewardTransactionEvent.class)))
                .thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload").build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionMapper).mapFromDTO(rtDTO);
        verify(rewardTransactionService).save(argThat((RewardTransactionEvent event) ->
                event.transaction() == rt));
    }

    @Test
    void executeShouldApplyUnifiedInvoiceReplacementEvent() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        rtDTO.setEventId("event-1");
        rtDTO.setSchemaVersion(1);
        rtDTO.setEventType("TRANSACTION_INVOICE_REPLACED");
        rtDTO.setOccurredAt(OffsetDateTime.parse("2026-08-01T00:30:00+02:00"));
        rtDTO.setTransactionRevision(6L);
        rtDTO.setStatus(SyncTrxStatus.INVOICED.name());
        rtDTO.setOperationType("PAYMENT");
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setTransactionRevision(6L);
        rt.setStatus(SyncTrxStatus.INVOICED.name());
        rt.setOperationType("PAYMENT");
        rt.setInitiatives(List.of("initiative-1"));

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(
                Mockito.any(RewardTransactionEvent.class)))
                .thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload").build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionService).save(argThat((RewardTransactionEvent event) ->
                event.eventId().equals("event-1")
                        && event.schemaVersion() == 1
                        && event.eventType().equals("TRANSACTION_INVOICE_REPLACED")
                        && event.occurredAt().equals(rtDTO.getOccurredAt())
                        && event.transactionRevision() == 6L
                        && event.transaction().getOperationType().equals("PAYMENT")));
        Mockito.verifyNoMoreInteractions(rewardTransactionService);
    }

    @Test
    void executeShouldMapAndSaveNonInvoicedTransaction() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.AUTHORIZED.name());
        rt.setInitiatives(List.of("initiative-1"));

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(Mockito.any(RewardTransactionEvent.class)))
                .thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload").build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionMapper).mapFromDTO(rtDTO);
        verify(rewardTransactionService).save(argThat((RewardTransactionEvent event) ->
                event.transaction() == rt));
    }
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidJsonPayloads")
    void invalidJsonAndIncompatibleFieldsAreNotifiedOnceWithoutPersistence(
            String description,
            String payload
    ) {
        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        Message<String> message = kafkaMessage(payload, 5L, acknowledgment);
        CapturingPersistenceTransactionMediator capturingMediator =
                capturingMediator(1_000, rewardTransactionMapper);
        capturingMediator.execute(Flux.just(message));

        StepVerifier.create(capturingMediator.afterCommits())
                .assertNext(results -> assertEquals(List.of(), results))
                .verifyComplete();

        Mockito.verifyNoInteractions(rewardTransactionMapper, rewardTransactionService);
        verify(transactionErrorNotifierService, Mockito.times(1))
                .notifyTransaction(same(message), Mockito.anyString(), eq(false), any(Throwable.class));
        verify(acknowledgment).acknowledge();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidInitiativePayloads")
    void malformedInvoicedInitiativesArePermanentBeforeServicePersistence(
            String description,
            String payload
    ) {
        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        Message<String> message = kafkaMessage(payload, 6L, acknowledgment);
        RewardTransactionMapper actualMapper = Mockito.spy(new RewardTransactionMapper());
        CapturingPersistenceTransactionMediator capturingMediator =
                capturingMediator(1_000, actualMapper);
        capturingMediator.execute(Flux.just(message));

        StepVerifier.create(capturingMediator.afterCommits())
                .assertNext(results -> assertEquals(List.of(), results))
                .verifyComplete();

        ArgumentCaptor<RewardTransactionDTO> transactionDto = ArgumentCaptor.forClass(RewardTransactionDTO.class);
        Mockito.verify(actualMapper, Mockito.times(1)).mapFromDTO(transactionDto.capture());
        if ("singular initiativeId".equals(description)) {
            assertNull(transactionDto.getValue().getInitiatives());
        }
        Mockito.verifyNoInteractions(rewardTransactionService);
        verify(transactionErrorNotifierService, Mockito.times(1))
                .notifyTransaction(
                        same(message),
                        Mockito.anyString(),
                        eq(false),
                        any(PermanentMessageException.class)
                );
        verify(acknowledgment).acknowledge();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("permanentFailures")
    void permanentProcessingFailuresAreAttemptedOnceAndNotifiedAsNonRetryable(
            String description,
            Throwable failure
    ) {
        when(rewardTransactionMapper.mapFromDTO(any(RewardTransactionDTO.class)))
                .thenReturn(validMappedTransaction());
        when(rewardTransactionService.save(any(RewardTransactionEvent.class)))
                .thenAnswer(invocation -> Mono.error(failure));

        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        Message<String> message = kafkaMessage(validTransactionPayload(), 7L, acknowledgment);

        StepVerifier.withVirtualTime(() -> {
                    CapturingPersistenceTransactionMediator capturingMediator =
                            capturingMediator(1_000, rewardTransactionMapper);
                    capturingMediator.execute(Flux.concat(Flux.just(message), Flux.never()));
                    return capturingMediator.afterCommits();
                })
                .thenRequest(Long.MAX_VALUE)
                .expectNoEvent(Duration.ofMillis(999))
                .then(() -> {
                    verify(rewardTransactionService, Mockito.times(1))
                            .save(any(RewardTransactionEvent.class));
                    verify(transactionErrorNotifierService, Mockito.times(1))
                            .notifyTransaction(
                                    same(message),
                                    Mockito.anyString(),
                                    eq(false),
                                    same(failure)
                            );
                    verify(acknowledgment, never()).acknowledge();
                })
                .thenAwait(Duration.ofMillis(1))
                .assertNext(results -> assertEquals(List.of(), results))
                .thenCancel()
                .verify();

        verify(rewardTransactionMapper, Mockito.times(1)).mapFromDTO(any(RewardTransactionDTO.class));
        verify(acknowledgment).acknowledge();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("retryableFailures")
    void unrelatedProcessingFailuresKeepFourAttemptsAndRetryableNotification(
            String description,
            Throwable failure
    ) {
        when(rewardTransactionMapper.mapFromDTO(any(RewardTransactionDTO.class)))
                .thenReturn(validMappedTransaction());
        when(rewardTransactionService.save(any(RewardTransactionEvent.class)))
                .thenAnswer(invocation -> Mono.error(failure));

        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        Message<String> message = kafkaMessage(validTransactionPayload(), 8L, acknowledgment);

        StepVerifier.withVirtualTime(() -> {
                    CapturingPersistenceTransactionMediator capturingMediator =
                            capturingMediator(60_000, rewardTransactionMapper);
                    capturingMediator.execute(Flux.concat(Flux.just(message), Flux.never()));
                    return capturingMediator.afterCommits();
                })
                .thenRequest(Long.MAX_VALUE)
                .expectNoEvent(Duration.ofSeconds(20))
                .then(() -> {
                    verify(rewardTransactionMapper, Mockito.times(4))
                            .mapFromDTO(any(RewardTransactionDTO.class));
                    verify(rewardTransactionService, Mockito.times(4))
                            .save(any(RewardTransactionEvent.class));
                    verify(transactionErrorNotifierService, Mockito.times(1))
                            .notifyTransaction(
                                    same(message),
                                    Mockito.anyString(),
                                    eq(true),
                                    same(failure)
                            );
                    verify(acknowledgment, never()).acknowledge();
                })
                .thenAwait(Duration.ofSeconds(40))
                .assertNext(results -> assertEquals(List.of(), results))
                .thenCancel()
                .verify();

        verify(acknowledgment).acknowledge();
    }

    @Test
    void uncommittableErrorIsNotRetriedNotNotifiedAndNotAcknowledged() {
        when(rewardTransactionMapper.mapFromDTO(any(RewardTransactionDTO.class)))
                .thenReturn(validMappedTransaction());
        UncommittableError failure = new UncommittableError("do not commit");
        when(rewardTransactionService.save(any(RewardTransactionEvent.class)))
                .thenReturn(Mono.error(failure));

        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        Message<String> message = kafkaMessage(validTransactionPayload(), 9L, acknowledgment);

        StepVerifier.withVirtualTime(() -> {
                    CapturingPersistenceTransactionMediator capturingMediator =
                            capturingMediator(1_000, rewardTransactionMapper);
                    capturingMediator.execute(Flux.just(message));
                    return capturingMediator.afterCommits();
                })
                .thenRequest(Long.MAX_VALUE)
                .expectErrorMatches(error -> error == failure)
                .verify();

        verify(rewardTransactionMapper, Mockito.times(1)).mapFromDTO(any(RewardTransactionDTO.class));
        verify(rewardTransactionService, Mockito.times(1)).save(any(RewardTransactionEvent.class));
        Mockito.verifyNoInteractions(transactionErrorNotifierService);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void permanentIncidentDoesNotBlockValidRecordAndAcknowledgesOnTheTimedPath() {
        String invalidPayload = """
                {"id":"incident-transaction","status":"INVOICED","initiativeId":"legacy-initiative"}
                """;
        String validPayload = validTransactionPayload();
        Acknowledgment invalidAcknowledgment = Mockito.mock(Acknowledgment.class);
        Acknowledgment validAcknowledgment = Mockito.mock(Acknowledgment.class);
        Message<String> invalidMessage = kafkaMessage(invalidPayload, 10L, invalidAcknowledgment);
        Message<String> validMessage = kafkaMessage(validPayload, 11L, validAcknowledgment);
        RewardTransactionMapper actualMapper = Mockito.spy(new RewardTransactionMapper());

        when(rewardTransactionService.save(any(RewardTransactionEvent.class)))
                .thenAnswer(invocation -> {
                    RewardTransactionEvent event = invocation.getArgument(0);
                    return Mono.just(event.transaction());
                });

        StepVerifier.withVirtualTime(() -> {
                    CapturingPersistenceTransactionMediator capturingMediator =
                            capturingMediator(1_000, actualMapper);
                    capturingMediator.execute(Flux.concat(
                            Flux.just(invalidMessage, validMessage),
                            Flux.never()
                    ));
                    return capturingMediator.afterCommits();
                })
                .thenRequest(Long.MAX_VALUE)
                .expectNoEvent(Duration.ofMillis(999))
                .then(() -> {
                    Mockito.verifyNoInteractions(invalidAcknowledgment, validAcknowledgment);
                    verify(transactionErrorNotifierService, Mockito.times(1))
                            .notifyTransaction(
                                    same(invalidMessage),
                                    Mockito.anyString(),
                                    eq(false),
                                    any(PermanentMessageException.class)
                            );
                    verify(rewardTransactionService, Mockito.times(1))
                            .save(any(RewardTransactionEvent.class));
                })
                .thenAwait(Duration.ofMillis(1))
                .assertNext(results -> {
                    assertEquals(1, results.size());
                    assertEquals("valid-transaction", results.getFirst().getId());
                    assertEquals(List.of("initiative-1"), results.getFirst().getInitiatives());
                })
                .thenCancel()
                .verify();

        verify(actualMapper, Mockito.times(2)).mapFromDTO(any(RewardTransactionDTO.class));
        verify(invalidAcknowledgment, never()).acknowledge();
        verify(validAcknowledgment).acknowledge();
    }

    @Test
    void otherApplicationRetryTest() {
        RewardTransactionDTO rtDT1 = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransactionDTO rtDT2 = RewardTransactionDTOFaker.mockInstance(2);

        Flux<Message<String>> msgs = Flux.just(rtDT1, rtDT2)
                .map(TestUtils::jsonSerializer)
                .map(payload -> MessageBuilder
                        .withPayload(payload)
                        .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                        .setHeader(KafkaHeaders.OFFSET, 0L)
                )
                .doOnNext(m -> m.setHeader(KafkaConstants.ERROR_MSG_HEADER_APPLICATION_NAME,
                        "otherAppName".getBytes(StandardCharsets.UTF_8)))
                .map(MessageBuilder::build);

        persistenceTransactionMediator.execute(msgs);

        Mockito.verifyNoInteractions(rewardTransactionMapper, rewardTransactionService, transactionErrorNotifierService);
    }

    @Test
    void executeShouldPersistMappedRefundedStatusAndIgnoreOperationTypeHeader() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        rtDTO.setStatus(SyncTrxStatus.REFUNDED.name());
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.REFUNDED.name());
        rt.setInitiatives(List.of("initiative-1"));

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(Mockito.any(RewardTransactionEvent.class)))
                .thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload")
                                .setHeader("operationType", "REFUNDED")
                                .build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionMapper).mapFromDTO(rtDTO);
        verify(rewardTransactionService).save(argThat((RewardTransactionEvent event) ->
                event.transaction() == rt));
    }

    @Test
    void executeShouldNotRewriteStatusFromOperationTypeHeader() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        rtDTO.setStatus(SyncTrxStatus.INVOICED.name());
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.INVOICED.name());
        rt.setInitiatives(List.of("initiative-1"));

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(Mockito.any(RewardTransactionEvent.class)))
                .thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload")
                                .setHeader("operationType", "REFUNDED")
                                .build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionService).save(argThat((RewardTransactionEvent event) ->
                SyncTrxStatus.INVOICED.name().equals(event.transaction().getStatus())));
    }

    @Test
    void getFlowNameShouldReturnTRANSACTION() {
        org.junit.jupiter.api.Assertions.assertEquals("TRANSACTION", persistenceTransactionMediator.getFlowName());
    }

    private static Stream<Arguments> invalidJsonPayloads() {
        return Stream.of(
                Arguments.of("invalid JSON", "{"),
                Arguments.of(
                        "incompatible amount type",
                        """
                                {"id":"bad-amount","amountCents":"not-a-number","initiatives":["initiative-1"]}
                                """
                )
        );
    }

    private static Stream<Arguments> invalidInitiativePayloads() {
        return Stream.of(
                Arguments.of(
                        "singular initiativeId",
                        """
                                {"id":"incident-transaction","status":"INVOICED","initiativeId":"legacy-initiative"}
                                """
                ),
                Arguments.of(
                        "missing initiatives",
                        """
                                {"id":"missing-initiatives","status":"INVOICED"}
                                """
                ),
                Arguments.of(
                        "null initiatives",
                        """
                                {"id":"null-initiatives","status":"INVOICED","initiatives":null}
                                """
                ),
                Arguments.of(
                        "empty initiatives",
                        """
                                {"id":"empty-initiatives","status":"INVOICED","initiatives":[]}
                                """
                ),
                Arguments.of(
                        "empty initiative value",
                        """
                                {"id":"empty-initiative","status":"INVOICED","initiatives":[""]}
                                """
                ),
                Arguments.of(
                        "blank initiative value",
                        """
                                {"id":"blank-initiative","status":"INVOICED","initiatives":["  "]}
                                """
                ),
                Arguments.of(
                        "null initiative element",
                        """
                                {"id":"null-initiative-element","status":"INVOICED","initiatives":[null]}
                                """
                ),
                Arguments.of(
                        "multiple initiatives",
                        """
                                {"id":"multiple-initiatives","status":"INVOICED",
                                 "initiatives":["initiative-1","initiative-2"]}
                                """
                )
        );
    }

    private static Stream<Arguments> permanentFailures() {
        return Stream.of(
                Arguments.of("direct permanent failure", new PermanentMessageException("invalid transaction")),
                Arguments.of(
                        "permanent failure with a retryable cause",
                        new PermanentMessageException("invalid transaction", new RuntimeException("cause"))
                ),
                Arguments.of(
                        "wrapped permanent failure",
                        new IllegalStateException(
                                "service wrapper",
                                new IllegalArgumentException(
                                        "adapter wrapper",
                                        new PermanentMessageException("invalid transaction")
                                )
                        )
                )
        );
    }

    private static Stream<Arguments> retryableFailures() {
        return Stream.of(
                Arguments.of("illegal argument", new IllegalArgumentException("bad argument")),
                Arguments.of("illegal state", new IllegalStateException("unexpected state")),
                Arguments.of(
                        "database failure",
                        new DataAccessResourceFailureException("database unavailable")
                ),
                Arguments.of("runtime failure", new RuntimeException("unexpected failure"))
        );
    }

    private CapturingPersistenceTransactionMediator capturingMediator(
            long commitMillis,
            RewardTransactionMapper mapper
    ) {
        return new CapturingPersistenceTransactionMediator(
                "appName",
                rewardTransactionService,
                transactionErrorNotifierService,
                mapper,
                commitMillis,
                new JsonConfig().objectMapper()
        );
    }

    private static Message<String> kafkaMessage(String payload, long offset) {
        return MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, offset)
                .build();
    }

    private static Message<String> kafkaMessage(
            String payload,
            long offset,
            Acknowledgment acknowledgment
    ) {
        return MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, offset)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment)
                .build();
    }

    private static String validTransactionPayload() {
        return """
                {"id":"valid-transaction","status":"AUTHORIZED","initiatives":["initiative-1"]}
                """;
    }

    private static RewardTransaction validMappedTransaction() {
        return RewardTransaction.builder()
                .id("valid-transaction")
                .status(SyncTrxStatus.AUTHORIZED.name())
                .initiatives(List.of("initiative-1"))
                .build();
    }

    private static class CapturingPersistenceTransactionMediator extends PersistenceTransactionMediatorImpl {

        private Flux<List<RewardTransaction>> afterCommits;

        private CapturingPersistenceTransactionMediator(
                String applicationName,
                RewardTransactionService rewardTransactionService,
                TransactionErrorNotifierService transactionErrorNotifierService,
                RewardTransactionMapper rewardTransactionMapper,
                long commitMillis,
                tools.jackson.databind.ObjectMapper objectMapper
        ) {
            super(
                    applicationName,
                    rewardTransactionService,
                    transactionErrorNotifierService,
                    rewardTransactionMapper,
                    commitMillis,
                    objectMapper
            );
        }

        @Override
        protected void subscribeAfterCommits(Flux<List<RewardTransaction>> afterCommits2subscribe) {
            this.afterCommits = afterCommits2subscribe;
        }

        private Flux<List<RewardTransaction>> afterCommits() {
            return afterCommits;
        }
    }
}
