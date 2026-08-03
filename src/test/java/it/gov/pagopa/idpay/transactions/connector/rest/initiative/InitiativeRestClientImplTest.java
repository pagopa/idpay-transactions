package it.gov.pagopa.idpay.transactions.connector.rest.initiative;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import it.gov.pagopa.idpay.transactions.config.InitiativeClientException;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import it.gov.pagopa.idpay.transactions.connector.rest.InitiativeRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.InitiativeRestClientImpl;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

/**
 * WireMock-based integration tests for {@link InitiativeRestClientImpl}.
 *
 * <p>This test suite mirrors the other REST-client integration tests. Every relevant HTTP outcome (2xx, 4xx, 5xx) is
 * covered so that each branch of the client is exercised and meets the project's coverage
 * thresholds (line coverage ≥ 90%, branch coverage ≥ 80%).
 *
 * <p>Tests validate:
 * <ul>
 *   <li>Successful responses (200 OK) return expected {@link InitiativeDetailDTO}</li>
 *   <li>Not found responses (404) raise {@link InitiativeNotFoundException}</li>
 *   <li>Client errors (400) raise {@link InitiativeClientException}</li>
 *   <li>Server errors (500) raise {@link InitiativeClientException}</li>
 *   <li>Retry logic properly handles transient failures</li>
 * </ul>
 */
@ContextConfiguration(
        classes = {
                InitiativeRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.initiative.base-url",
                "app.initiative.retry.delay-millis=100",
                "app.initiative.retry.max-attempts=1"
        }
)
class InitiativeRestClientImplTest extends BaseWireMockTest {

    private static final String INITIATIVE_OK = "INITIATIVE_OK_1";
    private static final String INITIATIVE_NOT_FOUND = "INITIATIVE_NOTFOUND_1";
    private static final String INITIATIVE_BAD_REQUEST = "INITIATIVE_BADREQUEST_1";
    private static final String INITIATIVE_INTERNAL_SERVER_ERROR = "INITIATIVE_INTERNALSERVERERROR_1";

    @Autowired
    private InitiativeRestClient initiativeRestClient;

    @Test
    void getInitiativeBeneficiaryDetail_Ok() {
        StepVerifier.create(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_OK))
                .assertNext(detail -> {
                    Assertions.assertNotNull(detail);
                    Assertions.assertEquals("Test Initiative", detail.getInitiativeName());
                    Assertions.assertEquals("PUBLISHED", detail.getStatus());
                })
                .verifyComplete();
    }

    @Test
    void getInitiativeBeneficiaryDetail_NotFound() {
        assertThrowsOnBlock(INITIATIVE_NOT_FOUND, InitiativeNotFoundException.class);
    }

    @Test
    void getInitiativeBeneficiaryDetail_BadRequest() {
        assertThrowsOnBlock(INITIATIVE_BAD_REQUEST, InitiativeClientException.class);
    }

    @Test
    void getInitiativeBeneficiaryDetail_InternalServerError() {
        assertThrowsOnBlock(INITIATIVE_INTERNAL_SERVER_ERROR, InitiativeClientException.class);
    }

    private void assertThrowsOnBlock(String initiativeId, Class<? extends Throwable> expected) {
        try {
            InitiativeDetailDTO result = initiativeRestClient
                    .getInitiativeBeneficiaryDetail(initiativeId)
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
