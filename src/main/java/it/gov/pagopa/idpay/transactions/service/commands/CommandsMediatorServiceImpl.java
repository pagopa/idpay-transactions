package it.gov.pagopa.idpay.transactions.service.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import it.gov.pagopa.common.reactive.kafka.consumer.BaseKafkaConsumer;
import it.gov.pagopa.idpay.transactions.dto.QueueCommandOperationDTO;
import it.gov.pagopa.idpay.transactions.service.TransactionErrorNotifierService;
import it.gov.pagopa.idpay.transactions.service.commands.ops.DeleteInitiativeService;
import it.gov.pagopa.idpay.transactions.utils.CommandsConstants;
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
public class CommandsMediatorServiceImpl extends BaseKafkaConsumer<QueueCommandOperationDTO, String> implements CommandsMediatorService {
    private final Duration commitDelay;
    private final DeleteInitiativeService deleteInitiativeService;
    private final TransactionErrorNotifierService transactionErrorNotifierService;
    private final ObjectReader objectReader;

    protected CommandsMediatorServiceImpl(
            @Value("${spring.application.name}") String applicationName,
            @Value("${spring.cloud.stream.kafka.bindings.consumerCommands-in-0.consumer.ackTime}")  long commitMillis,
            DeleteInitiativeService deleteInitiativeService, TransactionErrorNotifierService transactionErrorNotifierService,
            ObjectMapper objectMapper) {
        super(applicationName);
        this.commitDelay = Duration.ofMillis(commitMillis);
        this.deleteInitiativeService = deleteInitiativeService;
        this.transactionErrorNotifierService = transactionErrorNotifierService;
        this.objectReader = objectMapper.readerFor(QueueCommandOperationDTO.class);
    }

    @Override
    protected Duration getCommitDelay() {
        return commitDelay;
    }

    @Override
    protected void subscribeAfterCommits(Flux<List<String>> afterCommits2subscribe) {
        afterCommits2subscribe
                .subscribe(r -> log.info("[TRANSACTIONS_COMMANDS] Processed offsets committed successfully"));
    }

    @Override
    protected ObjectReader getObjectReader() {
        return objectReader;
    }

    @Override
    protected Consumer<Throwable> onDeserializationError(Message<String> message) {
        return e -> transactionErrorNotifierService.notifyTransactionCommands(message, "[TRANSACTIONS_COMMANDS] Unexpected JSON", false, e);
    }

    @Override
    protected void notifyError(Message<String> message, Throwable e) {
        transactionErrorNotifierService.notifyTransactionCommands(message, "[TRANSACTIONS_COMMANDS] An error occurred evaluating commands", true, e);
    }

    @Override
    protected Mono<String> execute(QueueCommandOperationDTO payload, Message<String> message, Map<String, Object> ctx) {
        if(CommandsConstants.COMMANDS_OPERATION_TYPE_DELETE_INITIATIVE.equals(payload.getOperationType())){
            return deleteInitiativeService.execute(payload);
        }
        log.debug("[TRANSACTIONS_COMMANDS] Not handled operation type {}", payload.getOperationType());
        return Mono.empty();
    }

    @Override
    public String getFlowName() {
        return "TRANSACTIONS_COMMANDS";
    }
}
