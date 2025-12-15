package it.gov.pagopa.idpay.transactions.notifier;

import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.cloud.stream.function.StreamBridge;
import reactor.core.publisher.Flux;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TransactionNotifierServiceImplTest {

    private StreamBridge streamBridge;
    private TransactionNotifierServiceImpl service;

    private static final String BINDER = "kafka";
    private static final String KEY = "test-key";

    @BeforeEach
    void setUp() {
        streamBridge = mock(StreamBridge.class);
        service = new TransactionNotifierServiceImpl(streamBridge, BINDER);
    }

    @Test
    void notify_shouldSendMessageAndReturnTrue() {
        RewardTransaction trx = new RewardTransaction();
        trx.setStatus("AUTHORIZED");

        when(streamBridge.send(eq("transactionOutcome-out-0"), eq(BINDER), any(Message.class)))
                .thenReturn(true);

        boolean result = service.notify(trx, KEY);

        assertThat(result).isTrue();

        verify(streamBridge, times(1))
                .send(eq("transactionOutcome-out-0"), eq(BINDER), any(Message.class));
    }

    @Test
    void buildMessage_shouldAddRefundedHeaderWhenStatusIsRefunded() {
        RewardTransaction trx = new RewardTransaction();
        trx.setStatus(SyncTrxStatus.REFUNDED.name());

        Message<RewardTransaction> message = service.buildMessage(trx, KEY);

        assertThat(message.getPayload()).isEqualTo(trx);
        assertThat(message.getHeaders().get(KafkaHeaders.KEY)).isEqualTo(KEY);
        assertThat(message.getHeaders().get("operationType")).isEqualTo("REFUNDED");
    }

    @Test
    void buildMessage_shouldNotAddOperationTypeHeaderWhenNotRefunded() {
        RewardTransaction trx = new RewardTransaction();
        trx.setStatus("AUTHORIZED");

        Message<RewardTransaction> message = service.buildMessage(trx, KEY);

        assertThat(message.getPayload()).isEqualTo(trx);
        assertThat(message.getHeaders().get(KafkaHeaders.KEY)).isEqualTo(KEY);
        assertThat(message.getHeaders().containsKey("operationType")).isFalse();
    }

    @Test
    void transactionOutcomeSupplier_shouldReturnEmptyFlux() {
        TransactionNotifierServiceImpl.TransactionNotifierServiceImplConfig config =
                new TransactionNotifierServiceImpl.TransactionNotifierServiceImplConfig();

        Supplier<Flux<Message<Object>>> supplier = config.transactionOutcome();

        Flux<Message<Object>> flux = supplier.get();

        assertThat(flux).isNotNull();
        assertThat(flux.collectList().block()).isEmpty();
    }
}
