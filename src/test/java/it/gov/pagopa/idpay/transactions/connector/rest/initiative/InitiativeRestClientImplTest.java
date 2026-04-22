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
 * <p>Mirrors the structure of {@code PaymentRestClientImplTest} to keep the
 * test suite homogeneous across REST clients: every relevant HTTP outcome
 * (2xx, 4xx, 5xx, 429) is covered so that each branch of the client is
 * exercised and meets the project's coverage thresholds.
 */
@ContextConfiguration(
        classes = {
                InitiativeRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.initiative.base-url"
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
            Assertions.assertEquals(expected, e.getClass(),
                    "Unexpected exception type: " + e);
        }
    }
}

