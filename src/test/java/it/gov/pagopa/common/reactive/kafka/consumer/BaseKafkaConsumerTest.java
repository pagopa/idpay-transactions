package it.gov.pagopa.common.reactive.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;

class BaseKafkaConsumerTest {

    @Test
    void subclassWithoutRetryOverrideKeepsBaseRetryPolicy() {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        Message<String> message = MessageBuilder.withPayload("\"transaction\"")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, 1L)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment)
                .build();
        RecordingConsumer consumer = new RecordingConsumer();

        StepVerifier.withVirtualTime(() -> {
                    consumer.execute(Flux.concat(Flux.just(message), Flux.never()));
                    return consumer.committedResults();
                })
                .thenRequest(Long.MAX_VALUE)
                .expectNoEvent(Duration.ofSeconds(20))
                .then(() -> {
                    assertEquals(4, consumer.attempts);
                    assertEquals(1, consumer.notifications.size());
                    assertSame(message, consumer.notifications.getFirst().message());
                    assertInstanceOf(IllegalStateException.class, consumer.notifications.getFirst().error());
                    verifyNoInteractions(acknowledgment);
                })
                .thenAwait(Duration.ofSeconds(40))
                .assertNext(results -> assertEquals(List.of(), results))
                .thenCancel()
                .verify();

        verify(acknowledgment).acknowledge();
    }

    private static class RecordingConsumer extends BaseKafkaConsumer<String, String> {

        private final ObjectReader objectReader = JsonMapper.builder().build().readerFor(String.class);
        private final List<Notification> notifications = new ArrayList<>();
        private int attempts;
        private Flux<List<String>> committedResults;

        private RecordingConsumer() {
            super("test-consumer");
        }

        @Override
        protected Duration getCommitDelay() {
            return Duration.ofSeconds(60);
        }

        @Override
        protected void subscribeAfterCommits(Flux<List<String>> afterCommits2subscribe) {
            committedResults = afterCommits2subscribe;
        }

        @Override
        protected ObjectReader getObjectReader() {
            return objectReader;
        }

        @Override
        protected Consumer<Throwable> onDeserializationError(Message<String> message) {
            return ignored -> {
            };
        }

        @Override
        protected void notifyError(Message<String> message, Throwable error) {
            notifications.add(new Notification(message, error));
        }

        @Override
        protected Mono<String> execute(String payload, Message<String> message, Map<String, Object> ctx) {
            return Mono.defer(() -> {
                attempts++;
                return Mono.error(new IllegalStateException("temporary failure"));
            });
        }

        private Flux<List<String>> committedResults() {
            return committedResults;
        }
    }

    private record Notification(Message<String> message, Throwable error) {
    }
}
