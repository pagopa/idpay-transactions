package it.gov.pagopa.common.reactive.stream.service;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ErrorPublisherImplTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void sendShouldDelegateToErrorsBindingAndPropagateResult(boolean sent) {
        StreamBridge streamBridge = mock(StreamBridge.class);
        Message<String> message = mock(Message.class);
        when(streamBridge.send("errors-out-0", message)).thenReturn(sent);

        boolean result = new ErrorPublisherImpl(streamBridge).send(message);

        assertEquals(sent, result);
        verify(streamBridge).send("errors-out-0", message);
    }

    @org.junit.jupiter.api.Test
    void errorsSupplierShouldExposeAnEmptyReactiveProducer() {
        Supplier<Flux<Message<Object>>> supplier =
                new ErrorPublisherImpl.ErrorNotifierProducerConfig().errors();

        StepVerifier.create(supplier.get()).verifyComplete();
    }
}
