package it.gov.pagopa.idpay.transactions.event;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.ErrorNotifierServiceImpl;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

class RewardTransactionConsumerTest extends BaseIntegrationTest {

    @Autowired
    private RewardTransactionRepository rewardTransactionRepository;

    @AfterEach
    void cleanData(){
        rewardTransactionRepository.deleteAll().block();
    }

    @Test
    void testRewardRuleBuilding(){
        int validTrx=1000; // use even number
        int notValidTrx = errorUseCases.size();
        long maxWaitingMs = 30000;

        List<String> transactionPayloads = new ArrayList<>(buildValidPayloads(errorUseCases.size(), validTrx / 2));
        transactionPayloads.addAll(IntStream.range(0, notValidTrx).mapToObj(i -> errorUseCases.get(i).getFirst().get()).toList());
        transactionPayloads.addAll(buildValidPayloads(errorUseCases.size() + (validTrx / 2) + notValidTrx, validTrx / 2));

        long timeStart=System.currentTimeMillis();
        transactionPayloads.forEach(i->publishIntoEmbeddedKafka(topicRewardedTrxRequest, null, null, i));
        publishIntoEmbeddedKafka(topicRewardedTrxRequest, List.of(new RecordHeader(ErrorNotifierServiceImpl.ERROR_MSG_HEADER_APPLICATION_NAME, "OTHERAPPNAME".getBytes(StandardCharsets.UTF_8))), null, "OTHERAPPMESSAGE");

        long timePublishingEnd=System.currentTimeMillis();

        long countSaved = waitForTrxStored(validTrx);
        long timeEnd=System.currentTimeMillis();

        Assertions.assertEquals(validTrx, countSaved);

        checkErrorsPublished(notValidTrx, maxWaitingMs, errorUseCases);

        System.out.printf("""
            ************************
            Time spent to send %d (%d + %d) messages (from start): %d millis
            Time spent to assert trx stored count (from previous check): %d millis
            ************************
            Test Completed in %d millis
            ************************
            """,
                validTrx + notValidTrx,
                validTrx,
                notValidTrx,
                timePublishingEnd-timeStart,
                timeEnd-timePublishingEnd,
                timeEnd-timeStart
        );

        long timeCommitCheckStart = System.currentTimeMillis();
        final Map<TopicPartition, OffsetAndMetadata> srcCommitOffsets = checkCommittedOffsets(topicRewardedTrxRequest, groupIdTransactionConsumer,transactionPayloads.size()+1); // +1 due to other applicationName useCase
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

    private List<String> buildValidPayloads(int bias, int n) {
        return IntStream.range(bias, bias + n)
                .mapToObj(RewardTransactionDTOFaker::mockInstance)
                .map(TestUtils::jsonSerializer)
                .toList();
    }

    private long waitForTrxStored(int N) {
        long[] countSaved={0};
        //noinspection ConstantConditions
        waitFor(()->(countSaved[0]=rewardTransactionRepository.count().block()) >= N, ()->"Expected %d saved rules, read %d".formatted(N, countSaved[0]), 60, 1000);
        return countSaved[0];
    }

    //region not valid useCases
    // all use cases configured must have a unique id recognized by the regexp getErrorUseCaseIdPatternMatch
    protected Pattern getErrorUseCaseIdPatternMatch() {
        return Pattern.compile("\"correlationId\":\"CORRELATIONID([0-9]+)\"");
    }
    private final List<Pair<Supplier<String>, Consumer<ConsumerRecord<String, String>>>> errorUseCases = new ArrayList<>();
    {
        String useCaseJsonNotExpected = "{\"correlationId\":\"CORRELATIONID0\",unexpectedStructure:0}";
        errorUseCases.add(Pair.of(
                () -> useCaseJsonNotExpected,
                errorMessage -> checkErrorMessageHeaders(errorMessage, "[TRANSACTION] Unexpected JSON", useCaseJsonNotExpected)
        ));

        String jsonNotValid = "{\"correlationId\":\"CORRELATIONID1\",invalidJson";
        errorUseCases.add(Pair.of(
                () -> jsonNotValid,
                errorMessage -> checkErrorMessageHeaders(errorMessage, "[TRANSACTION] Unexpected JSON", jsonNotValid)
        ));

    }

    private void checkErrorMessageHeaders(ConsumerRecord<String, String> errorMessage, String errorDescription, String expectedPayload) {
        checkErrorMessageHeaders(topicRewardedTrxRequest, groupIdTransactionConsumer, errorMessage, errorDescription, expectedPayload,true,true);
    }
    //endregion
}
