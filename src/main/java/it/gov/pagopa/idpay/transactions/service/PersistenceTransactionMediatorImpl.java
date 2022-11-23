package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
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
    private final ErrorNotifierService errorNotifierService;
    private final RewardTransactionMapper rewardTransactionMapper;

    private final Duration commitDelay;

    private final ObjectReader objectReader;

    public PersistenceTransactionMediatorImpl(
            @Value("${spring.application.name}") String applicationName,
            RewardTransactionService rewardTransactionService,
            ErrorNotifierService errorNotifierService,

            RewardTransactionMapper rewardTransactionMapper, @Value("${spring.cloud.stream.kafka.bindings.rewardTrxConsumer-in-0.consumer.ackTime}") long commitMillis,

            ObjectMapper objectMapper) {
        super(applicationName);
        this.rewardTransactionService = rewardTransactionService;
        this.errorNotifierService = errorNotifierService;
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
        afterCommits2subscribe.subscribe(p -> log.debug("[TRANSACTION] Processed offsets committed successfully"));
    }

    @Override
    protected ObjectReader getObjectReader() {
        return objectReader;
    }

    @Override
    protected Consumer<Throwable> onDeserializationError(Message<String> message) {
        return e -> errorNotifierService.notifyTransaction(message, "[TRANSACTION] Unexpected JSON", true, e);
    }

    @Override
    protected void notifyError(Message<String> message, Throwable e) {
        errorNotifierService.notifyTransaction(message, "[TRANSACTION] An error occurred evaluating transaction", true, e);
    }

    @Override
    protected Mono<RewardTransaction> execute(RewardTransactionDTO payload, Message<String> message, Map<String, Object> ctx) {
        return Mono.just(payload)
                .map(this.rewardTransactionMapper::mapFromDTO)
                .flatMap(this.rewardTransactionService::save);
    }

    @Override
    protected String getFlowName() {
        return "TRANSACTION";
    }
}
