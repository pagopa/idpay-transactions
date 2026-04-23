package it.gov.pagopa.idpay.transactions.test.fakers;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;

import java.time.*;
import java.util.Random;

public class RewardTransactionFaker {

    private RewardTransactionFaker() {
    }
    private static final Random randomGenerator = new Random();

    private static Random getRandom(Integer bias) {
        return bias == null ? randomGenerator : new Random(bias);
    }


    private static int getRandomPositiveNumber(Integer bias, int bound) {
        return Math.abs(getRandom(bias).nextInt(bound));
    }


    /**
     * It will return an example of {@link RewardTransactionDTO}. Providing a bias, it will return a pseudo-casual object
     */
    public static RewardTransaction mockInstance(Integer bias) {
        return mockInstanceBuilder(bias).build();
    }

    public static RewardTransaction.RewardTransactionBuilder mockInstanceBuilder(Integer bias) {
        LocalDate trxDate = LocalDate.of(2022, getRandomPositiveNumber(bias, 11) + 1, getRandomPositiveNumber(bias, 27)+1);
        LocalTime trxTime = LocalTime.of(getRandomPositiveNumber(bias, 23), getRandomPositiveNumber(bias, 59), getRandomPositiveNumber(bias, 59));
        Instant trxDateTime = OffsetDateTime.of(
                trxDate,
                trxTime,
                ZoneOffset.UTC
        ).toInstant();


        return RewardTransaction.builder()
                .idTrxAcquirer("IDTRXACQUIRER%s".formatted(bias))
                .acquirerCode("ACQUIRERCODE%s".formatted(bias))
                .trxDate(trxDateTime)
                .hpan("HPAN%s".formatted(bias))
                .operationType("OPERATIONTYPE%s".formatted(bias))
                .circuitType("CIRCUITTYPE%s".formatted(bias))
                .idTrxIssuer("IDTRXISSUER%s".formatted(bias))
                .correlationId("CORRELATIONID%s".formatted(bias))
                .amountCents(getRandomPositiveNumber(bias, 200)*100L)
                .amountCurrency("AMOUNTCURRENCY%s".formatted(bias))
                .mcc("MCC%s".formatted(bias))
                .acquirerId("ACQUIRERID%s".formatted(bias))
                .merchantId("MERCHANTID%s".formatted(bias))
                .pointOfSaleId("POINTOFSALEID%s".formatted(bias))
                .terminalId("TERMINALID%s".formatted(bias))
                .bin("BIN%s".formatted(bias))
                .senderCode("SENDERCODE%s".formatted(bias))
                .fiscalCode("FISCALCODE%s".formatted(bias))
                .vat("VAT%s".formatted(bias))
                .posType("POSTYPE%s".formatted(bias))
                .par("PAR%s".formatted(bias))
                .userId("USERID%s".formatted(bias))
                .channel("CHANNEL%d".formatted(bias));
    }
}

