package it.gov.pagopa.idpay.transactions.event;

import com.mongodb.MongoException;
import it.gov.pagopa.common.utils.TestUtils;
import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.dto.QueueCommandOperationDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import it.gov.pagopa.idpay.transactions.utils.CommandsConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.util.Pair;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

@TestPropertySource(properties = {
        "logging.level.it.gov.pagopa.idpay.transactions.service.commands.CommandsMediatorServiceImpl=WARN",
        "logging.level.it.gov.pagopa.idpay.transactions.service.commands.ops.DeleteInitiativeServiceImpl=WARN",
})
class CommandsConsumerConfigTest extends BaseIntegrationTest {
    private static final int VALID_USE_CASES = 3;
    private static final int NUMBER_TRX = 3;
    private final String INITIATIVEID = "INITIATIVEID_%d";
    private final Set<String> INITIATIVES_DELETED = new HashSet<>();
    @SpyBean
    private RewardTransactionRepository rewardTransactionRepository;

    @Test
    void test() {
        int validMessages = 100;
        int notValidMessages = errorUseCases.size();
        long maxWaitingMs = 30000;

        List<String> commandsPayloads = new ArrayList<>(notValidMessages+validMessages);
        commandsPayloads.addAll(IntStream.range(0,notValidMessages).mapToObj(i -> errorUseCases.get(i).getFirst().get()).toList());
        commandsPayloads.addAll(buildValidPayloads(notValidMessages, notValidMessages+validMessages));

        long timeStart=System.currentTimeMillis();
        commandsPayloads.forEach(cp -> kafkaTestUtilitiesService.publishIntoEmbeddedKafka(topicCommands, null, null, cp));
        long timePublishingEnd = System.currentTimeMillis();

        waitForLastStorageChange((validMessages/VALID_USE_CASES)*2*NUMBER_TRX);

        long timeEnd=System.currentTimeMillis();

        checkRepositories();
        checkErrorsPublished(notValidMessages, maxWaitingMs, errorUseCases);

        System.out.printf("""
                        ************************
                        Time spent to send %d (%d + %d) messages (from start): %d millis
                        Time spent to assert db stored count (from previous check): %d millis
                        ************************
                        Test Completed in %d millis
                        ************************
                        """,
                commandsPayloads.size(),
                validMessages,
                notValidMessages,
                timePublishingEnd - timeStart,
                timeEnd - timePublishingEnd,
                timeEnd - timeStart
        );

        long timeCommitCheckStart = System.currentTimeMillis();
        Map<TopicPartition, OffsetAndMetadata> srcCommitOffsets = kafkaTestUtilitiesService.checkCommittedOffsets(topicCommands, groupIdCommands, commandsPayloads.size());
        long timeCommitCheckEnd = System.currentTimeMillis();

        System.out.printf("""
                        ************************
                        Time occurred to check committed offset: %d millis
                        ************************
                        Source Topic Committed Offsets: %s
                        ************************
                        """,
                timeCommitCheckEnd - timeCommitCheckStart,
                srcCommitOffsets
        );

    }


    private long waitForLastStorageChange(int n) {
        long[] countSaved={0};
        //noinspection ConstantConditions
        TestUtils.waitFor(()->(countSaved[0]=rewardTransactionRepository.findAll().count().block()) == n, ()->"Expected %d saved transactions in db, read %d".formatted(n, countSaved[0]), 60, 1000);
        return countSaved[0];
    }

    private List<String> buildValidPayloads(int startValue, int messagesNumber) {
        return IntStream.range(startValue, messagesNumber)
                .mapToObj(i -> {
                    QueueCommandOperationDTO command = QueueCommandOperationDTO.builder()
                            .entityId(INITIATIVEID.formatted(i))
                            .operationTime(LocalDateTime.now())
                            .build();
                    switch (i%VALID_USE_CASES){
                        case 0 -> {
                            INITIATIVES_DELETED.add(command.getEntityId());
                            command.setOperationType(CommandsConstants.COMMANDS_OPERATION_TYPE_DELETE_INITIATIVE);
                            initializeDB(i, "QRCODE");
                        }
                        case 1 -> {
                            INITIATIVES_DELETED.add(command.getEntityId());
                            command.setOperationType(CommandsConstants.COMMANDS_OPERATION_TYPE_DELETE_INITIATIVE);
                            initializeDB(i, "RTD");
                        }
                        default -> {
                            command.setOperationType("ANOTHER_TYPE");
                            initializeDB(i, "QRCODE");
                        }
                    }
                    return command;
                })
                .map(TestUtils::jsonSerializer)
                .toList();
    }

    private void initializeDB(int bias, String channel) {
        Map<String, List<String>> initiativeRejectionReasons = new HashMap<>();
        initiativeRejectionReasons.put(INITIATIVEID.formatted(bias), List.of("BUDGET_EXHAUSTED"));

        List<RewardTransaction> rewardTransactionList = new ArrayList<>();
        IntStream.range(1, NUMBER_TRX+1).forEach(i -> rewardTransactionList.add(
                RewardTransactionFaker.mockInstanceBuilder(i)
                        .id("id_%d_%d".formatted(i, bias))
                        .initiatives(List.of(INITIATIVEID.formatted(bias)))
                        .initiativeRejectionReasons(initiativeRejectionReasons)
                        .channel(channel)
                        .build()));

        rewardTransactionRepository.saveAll(rewardTransactionList).blockLast();
    }

    protected Pattern getErrorUseCaseIdPatternMatch() {
        return Pattern.compile("\"entityId\":\"ENTITYID_ERROR([0-9]+)\"");
    }

    private final List<Pair<Supplier<String>, Consumer<ConsumerRecord<String, String>>>> errorUseCases = new ArrayList<>();

    {
        String useCaseJsonNotExpected = "{\"entityId\":\"ENTITYID_ERROR0\",unexpectedStructure:0}";
        errorUseCases.add(Pair.of(
                () -> useCaseJsonNotExpected,
                errorMessage -> checkErrorMessageHeaders(errorMessage, "[TRANSACTIONS_COMMANDS] Unexpected JSON", useCaseJsonNotExpected)
        ));

        String jsonNotValid = "{\"entityId\":\"ENTITYID_ERROR1\",invalidJson";
        errorUseCases.add(Pair.of(
                () -> jsonNotValid,
                errorMessage -> checkErrorMessageHeaders(errorMessage, "[TRANSACTIONS_COMMANDS] Unexpected JSON", jsonNotValid)
        ));

        final String errorInitiativeId = "ENTITYID_ERROR2";
        QueueCommandOperationDTO commandOperationError = QueueCommandOperationDTO.builder()
                .entityId(errorInitiativeId)
                .operationType(CommandsConstants.COMMANDS_OPERATION_TYPE_DELETE_INITIATIVE)
                .operationTime(LocalDateTime.now())
                .build();
        String commandOperationErrorString = TestUtils.jsonSerializer(commandOperationError);
        errorUseCases.add(Pair.of(
                () -> {
                    Mockito.doThrow(new MongoException("Command error dummy delete"))
                            .when(rewardTransactionRepository).findOneByInitiativeId(errorInitiativeId);
                    return commandOperationErrorString;
                },
                errorMessage -> checkErrorMessageHeaders(errorMessage, "[TRANSACTIONS_COMMANDS] An error occurred evaluating commands", commandOperationErrorString)
        ));
    }

    private void checkRepositories() {
        Assertions.assertTrue(rewardTransactionRepository.findAll().toStream().noneMatch(ri -> ri.getInitiatives().containsAll(INITIATIVES_DELETED)));
    }

    private void checkErrorMessageHeaders(ConsumerRecord<String, String> errorMessage, String errorDescription, String expectedPayload) {
        checkErrorMessageHeaders(topicCommands, groupIdCommands, errorMessage, errorDescription, expectedPayload, null);
    }
}