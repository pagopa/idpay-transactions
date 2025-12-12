package it.gov.pagopa.idpay.transactions.notifier;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class TransactionNotifierServiceImpl implements TransactionNotifierService {

  private String binder;

  private final StreamBridge streamBridge;

  public TransactionNotifierServiceImpl(StreamBridge streamBridge,@Value("${spring.cloud.stream.bindings.transactionOutcome-out-0.binder}") String binder ) {
    this.streamBridge = streamBridge;
    this.binder=binder;
  }

  @Configuration
  static class TransactionNotifierServiceImplConfig {
    @Bean
    public Supplier<Flux<Message<Object>>> transactionOutcome() {
      return Flux::empty;
    }
  }

  @Override
  public boolean notify(RewardTransaction trx, String key) {
    return streamBridge.send("transactionOutcome-out-0", binder, buildMessage(trx, key));
  }

  @Override
  public Message<RewardTransaction> buildMessage(RewardTransaction trx, String key) {
    return MessageBuilder.withPayload(trx)
        .setHeader(KafkaHeaders.KEY, key)
        .build();
  }
}
