package it.gov.pagopa.idpay.transactions.connector.rest.payment;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import it.gov.pagopa.idpay.transactions.connector.rest.PaymentRestClientImpl;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.util.Set;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;

@ContextConfiguration(
        classes = {
                PaymentRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.payment.base-url",
                "app.payment.retry.delay-millis=100",
                "app.payment.retry.max-attempts=1"
        }
)
class PaymentRestClientImplTest extends BaseWireMockTest {

    private static final Set<String> TRX_OK = Set.of("TRX_OK_1", "TRX_OK_2");
    private static final Set<String> TRX_BAD_REQUEST = Set.of("TRX_BADREQUEST_1");
    private static final Set<String> TRX_INTERNAL_SERVER_ERROR = Set.of("TRX_INTERNALSERVERERROR_1");

    @Autowired
    private PaymentRestClientImpl paymentRestClient;

    @Test
    void updateTransactionsStatus_Ok() {
        StepVerifier.create(paymentRestClient.updateTransactionsStatus(TRX_OK, SyncTrxStatus.REWARDED))
                .assertNext(updatedCount -> {
                    Assertions.assertNotNull(updatedCount);
                    Assertions.assertEquals(2, updatedCount);
                })
                .verifyComplete();
    }

    @Test
    void updateTransactionsStatus_BadRequest() {
        assertThrowsOnBlock(TRX_BAD_REQUEST, SyncTrxStatus.REWARDED, IllegalStateException.class);
    }

    @Test
    void updateTransactionsStatus_InternalServerError() {
        assertThrowsOnBlock(TRX_INTERNAL_SERVER_ERROR, SyncTrxStatus.REWARDED, IllegalStateException.class);
    }

    private void assertThrowsOnBlock(Set<String> transactionIds, SyncTrxStatus status, Class<? extends Throwable> expected) {
        try {
            Integer result = paymentRestClient
                    .updateTransactionsStatus(transactionIds, status)
                    .block();
            Assertions.fail("Expected " + expected.getSimpleName() + " but got result=" + result);
        } catch (Throwable e) {
            boolean isExpectedOrCausedByExpected = isThrowableOfTypeOrCausedBy(e, expected);
            Assertions.assertTrue(isExpectedOrCausedByExpected,
                    "Unexpected exception type: " + e + ". Expected: " + expected.getSimpleName());
        }
    }

    private boolean isThrowableOfTypeOrCausedBy(Throwable throwable, Class<? extends Throwable> expectedType) {
        if (throwable == null) {
            return false;
        }
        if (expectedType.isInstance(throwable)) {
            return true;
        }
        Throwable cause = throwable.getCause();
        while (cause != null) {
            if (expectedType.isInstance(cause)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}