package it.gov.pagopa.idpay.transactions.service;

import static it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision.ALLOWED;
import static it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision.DENIED;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision;
import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleOperation;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.persistence.port.PaymentRewardBatchImpactPort;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class InvoiceLifecycleEligibilityServiceImplTest {

    private static final String MERCHANT_ID = "merchant";
    private static final String TRANSACTION_ID = "transaction";
    private static final String BASIC_SCOPE = "transaction:invoicelifecycle:basic";
    private static final String FULL_SCOPE = "transaction:invoicelifecycle:full";

    private static final Set<Cell> POINT_OF_SALE_ALLOWED = Set.of(
            new Cell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.CONSULTABLE),
            new Cell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.SUSPENDED)
    );

    private static final Set<Cell> MERCHANT_ALLOWED = Set.of(
            new Cell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.CONSULTABLE),
            new Cell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.SUSPENDED),
            new Cell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.CONSULTABLE),
            new Cell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.TO_CHECK),
            new Cell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.SUSPENDED),
            new Cell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.REJECTED),
            new Cell(RewardBatchStatus.APPROVED, RewardBatchTrxStatus.REJECTED),
            new Cell(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.REJECTED),
            new Cell(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.REJECTED),
            new Cell(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.REJECTED)
    );

    private static final RewardBatchStatus[] PERSISTED_BATCH_STATUSES = {
            RewardBatchStatus.CREATED,
            RewardBatchStatus.SENT,
            RewardBatchStatus.EVALUATING,
            RewardBatchStatus.APPROVING,
            RewardBatchStatus.APPROVED,
            RewardBatchStatus.PENDING_REFUND,
            RewardBatchStatus.REFUNDED,
            RewardBatchStatus.NOT_REFUNDED
    };

    @Mock
    private PaymentRewardBatchImpactPort paymentRewardBatchImpactPort;

    private InvoiceLifecycleEligibilityServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new InvoiceLifecycleEligibilityServiceImpl(paymentRewardBatchImpactPort);
    }

    @ParameterizedTest(name = "{0} {1} {2}/{3} is {4}")
    @MethodSource("authoritativeMatrix")
    void evaluatesEveryAuthoritativeMatrixCell(
            InvoiceLifecycleOperation operation,
            Actor actor,
            RewardBatchStatus batchStatus,
            RewardBatchTrxStatus transactionStatus,
            InvoiceLifecycleEligibilityDecision expected
    ) {
        when(paymentRewardBatchImpactPort.findEligibility(MERCHANT_ID, TRANSACTION_ID))
                .thenReturn(Mono.just(eligibility(batchStatus, transactionStatus)));

        StepVerifier.create(service.evaluate(
                        MERCHANT_ID,
                        TRANSACTION_ID,
                        operation,
                        bearer(actor.scope)
                ))
                .expectNext(expected)
                .verifyComplete();

        verify(paymentRewardBatchImpactPort).findEligibility(MERCHANT_ID, TRANSACTION_ID);
    }

    @Test
    void merchantScopeTakesPrecedenceWhenBothAuthoritiesArePresent() {
        when(paymentRewardBatchImpactPort.findEligibility(MERCHANT_ID, TRANSACTION_ID))
                .thenReturn(Mono.just(eligibility(
                        RewardBatchStatus.EVALUATING,
                        RewardBatchTrxStatus.CONSULTABLE
                )));

        StepVerifier.create(service.evaluate(
                        MERCHANT_ID,
                        TRANSACTION_ID,
                        InvoiceLifecycleOperation.INVOICE_REPLACEMENT,
                        bearer(BASIC_SCOPE + " " + FULL_SCOPE)
                ))
                .expectNext(ALLOWED)
                .verifyComplete();
    }

    @Test
    void allowsWhenNoBatchMembershipExists() {
        when(paymentRewardBatchImpactPort.findEligibility(MERCHANT_ID, TRANSACTION_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.evaluate(
                        MERCHANT_ID,
                        TRANSACTION_ID,
                        InvoiceLifecycleOperation.INVOICED_REVERSAL,
                        bearer(FULL_SCOPE)
                ))
                .expectNext(ALLOWED)
                .verifyComplete();
    }

    @Test
    void rejectsUnsupportedOperationBeforeReadingMembership() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.evaluate(
                        MERCHANT_ID,
                        TRANSACTION_ID,
                        null,
                        bearer(FULL_SCOPE)
                )
        );
        verifyNoInteractions(paymentRewardBatchImpactPort);
    }

    @Test
    void rejectsCallerWithoutSupportedAuthority() {
        assertThrows(
                ResponseStatusException.class,
                () -> service.evaluate(
                        MERCHANT_ID,
                        TRANSACTION_ID,
                        InvoiceLifecycleOperation.INVOICE_REPLACEMENT,
                        bearer("transaction:read")
                )
        );
        verifyNoInteractions(paymentRewardBatchImpactPort);
    }

    @ParameterizedTest(name = "{0}/{1} fails closed")
    @MethodSource("failClosedStates")
    void virtualAndMissingStatesFailClosed(
            RewardBatchStatus batchStatus,
            RewardBatchTrxStatus transactionStatus
    ) {
        when(paymentRewardBatchImpactPort.findEligibility(MERCHANT_ID, TRANSACTION_ID))
                .thenReturn(Mono.just(eligibility(batchStatus, transactionStatus)));

        StepVerifier.create(service.evaluate(
                        MERCHANT_ID,
                        TRANSACTION_ID,
                        InvoiceLifecycleOperation.INVOICE_REPLACEMENT,
                        bearer(FULL_SCOPE)
                ))
                .expectNext(DENIED)
                .verifyComplete();
    }

    private static Stream<Arguments> authoritativeMatrix() {
        return Stream.of(InvoiceLifecycleOperation.values())
                .flatMap(operation -> Stream.of(Actor.values())
                        .flatMap(actor -> Stream.of(PERSISTED_BATCH_STATUSES)
                                .flatMap(batchStatus -> Stream.of(RewardBatchTrxStatus.values())
                                        .map(transactionStatus -> {
                                            Cell cell = new Cell(batchStatus, transactionStatus);
                                            boolean allowed = actor == Actor.MERCHANT
                                                    ? MERCHANT_ALLOWED.contains(cell)
                                                    : POINT_OF_SALE_ALLOWED.contains(cell);
                                            return Arguments.of(
                                                    operation,
                                                    actor,
                                                    batchStatus,
                                                    transactionStatus,
                                                    allowed ? ALLOWED : DENIED
                                            );
                                        }))));
    }

    private static Stream<Arguments> failClosedStates() {
        return Stream.of(
                Arguments.of(RewardBatchStatus.TO_WORK, RewardBatchTrxStatus.CONSULTABLE),
                Arguments.of(RewardBatchStatus.TO_APPROVE, RewardBatchTrxStatus.REJECTED),
                Arguments.of(null, RewardBatchTrxStatus.CONSULTABLE),
                Arguments.of(RewardBatchStatus.CREATED, null)
        );
    }

    private static PaymentBatchEligibility eligibility(
            RewardBatchStatus batchStatus,
            RewardBatchTrxStatus transactionStatus
    ) {
        return new PaymentBatchEligibility(
                TRANSACTION_ID,
                "initiative",
                MERCHANT_ID,
                "reward-batch",
                "INVOICED",
                batchStatus,
                transactionStatus
        );
    }

    private static String bearer(String scopes) {
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"scope\":\"" + scopes + "\"}").getBytes(StandardCharsets.UTF_8)
        );
        return "Bearer header." + payload + ".signature";
    }

    private enum Actor {
        POINT_OF_SALE(BASIC_SCOPE),
        MERCHANT(FULL_SCOPE);

        private final String scope;

        Actor(String scope) {
            this.scope = scope;
        }
    }

    private record Cell(
            RewardBatchStatus batchStatus,
            RewardBatchTrxStatus transactionStatus
    ) {
    }
}
