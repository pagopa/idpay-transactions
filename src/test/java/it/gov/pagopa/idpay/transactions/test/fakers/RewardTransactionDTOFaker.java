package it.gov.pagopa.idpay.transactions.test.fakers;

import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

public class RewardTransactionDTOFaker {
    private RewardTransactionDTOFaker() {
    }

    private static final Random randomGenerator = new Random();

    private static Random getRandom(Integer bias) {
        return bias == null ? randomGenerator : new Random(bias);
    }

    private static int getRandomPositiveNumber(Integer bias) {
        return Math.abs(getRandom(bias).nextInt());
    }

    private static int getRandomPositiveNumber(Integer bias, int bound) {
        return Math.abs(getRandom(bias).nextInt(bound));
    }

    private static final FakeValuesService fakeValuesServiceGlobal = new FakeValuesService(new Locale("it"), new RandomService(null));

    private static FakeValuesService getFakeValuesService(Integer bias) {
        return bias == null ? fakeValuesServiceGlobal : new FakeValuesService(new Locale("it"), new RandomService(getRandom(bias)));
    }

    /**
     * It will return an example of {@link RewardTransactionDTO}. Providing a bias, it will return a pseudo-casual object
     */
    public static RewardTransactionDTO mockInstance(Integer bias) {
        return mockInstanceBuilder(bias).build();
    }

    public static RewardTransactionDTO.RewardTransactionDTOBuilder mockInstanceBuilder(Integer bias) {
        LocalDate trxDate = LocalDate.of(2022, getRandomPositiveNumber(bias, 11) + 1, getRandomPositiveNumber(bias, 27)+1);
        LocalTime trxTime = LocalTime.of(getRandomPositiveNumber(bias, 23), getRandomPositiveNumber(bias, 59), getRandomPositiveNumber(bias, 59));
        LocalDateTime trxDateTime = LocalDateTime.of(trxDate, trxTime);

        RewardTransactionDTO.RewardTransactionDTOBuilder out = RewardTransactionDTO.builder();

        out.idTrxAcquirer("IDTRXACQUIRER%d".formatted(bias));
        out.acquirerCode("ACQUIRERCODE%d".formatted(bias));
        out.trxDate(OffsetDateTime.of(
                trxDateTime,
                ZoneId.of("Europe/Rome").getRules().getOffset(trxDateTime)));
        out.hpan("HPAN%s".formatted(bias));
        out.operationType("OPERATIONTYPE%d".formatted(bias));
        out.circuitType("CIRCUITTYPE%d".formatted(bias));
        out.idTrxIssuer("IDTRXISSUER%d".formatted(bias));
        out.correlationId("CORRELATIONID%d".formatted(bias));
        out.amount(BigDecimal.valueOf(getRandomPositiveNumber(bias, 200)));
        out.amountCurrency("AMOUNTCURRENCY%d".formatted(bias));
        out.mcc("MCC%d".formatted(bias));
        out.acquirerId("ACQUIRERID%d".formatted(bias));
        out.merchantId("MERCHANTID%d".formatted(bias));
        out.terminalId("TERMINALID%d".formatted(bias));
        out.bin("BIN%d".formatted(bias));
        out.senderCode("SENDERCODE%d".formatted(bias));
        out.fiscalCode("FISCALCODE%d".formatted(bias));
        out.vat("VAT%d".formatted(bias));
        out.posType("POSTYPE%d".formatted(bias));
        out.par("PAR%d".formatted(bias));
        out.status("STATUS%d".formatted(bias));
        out.userId("USERID%d".formatted(bias));
        out.maskedPan("MASKEDPAN%d".formatted(bias));
        out.brandLogo("BRANDLOGO%d".formatted(bias));
        return out;
    }
}
