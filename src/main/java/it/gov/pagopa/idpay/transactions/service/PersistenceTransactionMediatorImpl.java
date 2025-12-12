package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import it.gov.pagopa.common.reactive.kafka.consumer.BaseKafkaConsumer;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Service
@Slf4j
public class PersistenceTransactionMediatorImpl extends BaseKafkaConsumer<RewardTransactionDTO, RewardTransaction> implements PersistenceTransactionMediator {

    private final RewardTransactionService rewardTransactionService;
    private final TransactionErrorNotifierService transactionErrorNotifierService;
    private final RewardTransactionMapper rewardTransactionMapper;
    private static final String OPERATION_TYPE_HEADER = "operationType";
    private static final String OPERATION_TYPE_REFUNDED = "REFUNDED";


  private final Duration commitDelay;

    private final ObjectReader objectReader;

    public PersistenceTransactionMediatorImpl(
            @Value("${spring.application.name}") String applicationName,
            RewardTransactionService rewardTransactionService,
            TransactionErrorNotifierService transactionErrorNotifierService,

            RewardTransactionMapper rewardTransactionMapper, @Value("${spring.cloud.stream.kafka.bindings.rewardTrxConsumer-in-0.consumer.ackTime}") long commitMillis,

            ObjectMapper objectMapper) {
        super(applicationName);
        this.rewardTransactionService = rewardTransactionService;
        this.transactionErrorNotifierService = transactionErrorNotifierService;
        this.rewardTransactionMapper = rewardTransactionMapper;
        this.commitDelay = Duration.ofMillis(commitMillis);

        this.objectReader = objectMapper.readerFor(RewardTransactionDTO.class);
    }

    @Override
    protected Duration getCommitDelay() {
        return commitDelay;
    }

    @Override
    protected void subscribeAfterCommits(Flux<List<RewardTransaction>> afterCommits2subscribe) {
        afterCommits2subscribe.subscribe(p -> log.info("[TRANSACTION] Processed offsets committed successfully"));
    }

    @Override
    protected ObjectReader getObjectReader() {
        return objectReader;
    }

    @Override
    protected Consumer<Throwable> onDeserializationError(Message<String> message) {
        return e -> transactionErrorNotifierService.notifyTransaction(message, "[TRANSACTION] Unexpected JSON", true, e);
    }

    @Override
    protected void notifyError(Message<String> message, Throwable e) {
        transactionErrorNotifierService.notifyTransaction(message, "[TRANSACTION] An error occurred evaluating transaction", true, e);
    }

  @Override
  protected Mono<RewardTransaction> execute(RewardTransactionDTO payload,
      Message<String> message,
      Map<String, Object> ctx) {

    Object opTypeHeader = message.getHeaders().get(OPERATION_TYPE_HEADER);

    if (OPERATION_TYPE_REFUNDED.equals(opTypeHeader)) {
      log.info("[REWARD-TRANSACTION-CONSUMER] Skipping REFUNDED transaction with id {}", payload.getId());
      return Mono.empty();

    }

    return Mono.just(payload)
        .map(this.rewardTransactionMapper::mapFromDTO)
        .flatMap(this.rewardTransactionService::save);
  }

  @Override
    public String getFlowName() {
        return "TRANSACTION";
    }
}
