package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision;
import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleOperation;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.persistence.port.PaymentRewardBatchImpactPort;
import it.gov.pagopa.idpay.transactions.utils.JwtUtils;
import java.util.List;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@Service
public class InvoiceLifecycleEligibilityServiceImpl implements InvoiceLifecycleEligibilityService {

    private static final String BASIC_SCOPE = "transaction:invoicelifecycle:basic";
    private static final String FULL_SCOPE = "transaction:invoicelifecycle:full";

    private static final Set<MatrixCell> POINT_OF_SALE_ALLOWED_CELLS = Set.of(
            new MatrixCell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.CONSULTABLE),
            new MatrixCell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.SUSPENDED)
    );

    private static final Set<MatrixCell> MERCHANT_ALLOWED_CELLS = Set.of(
            new MatrixCell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.CONSULTABLE),
            new MatrixCell(RewardBatchStatus.CREATED, RewardBatchTrxStatus.SUSPENDED),
            new MatrixCell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.CONSULTABLE),
            new MatrixCell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.TO_CHECK),
            new MatrixCell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.SUSPENDED),
            new MatrixCell(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.REJECTED),
            new MatrixCell(RewardBatchStatus.APPROVED, RewardBatchTrxStatus.REJECTED),
            new MatrixCell(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.REJECTED),
            new MatrixCell(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.REJECTED),
            new MatrixCell(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.REJECTED)
    );

    private final PaymentRewardBatchImpactPort paymentRewardBatchImpactPort;

    public InvoiceLifecycleEligibilityServiceImpl(
            PaymentRewardBatchImpactPort paymentRewardBatchImpactPort
    ) {
        this.paymentRewardBatchImpactPort = paymentRewardBatchImpactPort;
    }

    @Override
    public Mono<InvoiceLifecycleEligibilityDecision> evaluate(
            String merchantId,
            String transactionId,
            InvoiceLifecycleOperation operation,
            String authorization
    ) {
        validateOperation(operation);
        Actor actor = resolveActor(JwtUtils.extractScopesOrThrow(authorization));

        return paymentRewardBatchImpactPort.findEligibility(merchantId, transactionId)
                .map(eligibility -> decision(actor, eligibility))
                .defaultIfEmpty(InvoiceLifecycleEligibilityDecision.ALLOWED);
    }

    private static void validateOperation(InvoiceLifecycleOperation operation) {
        if (operation == null) {
            throw new IllegalArgumentException("Invoice lifecycle operation is required");
        }
        switch (operation) {
            case INVOICE_REPLACEMENT, INVOICED_REVERSAL -> {
                // Both governed operations currently share the same matrix.
            }
        }
    }

    private static Actor resolveActor(List<String> scopes) {
        if (scopes.contains(FULL_SCOPE)) {
            return Actor.MERCHANT;
        }
        if (scopes.contains(BASIC_SCOPE)) {
            return Actor.POINT_OF_SALE;
        }
        throw new ResponseStatusException(
                HttpStatus.FORBIDDEN,
                "A supported invoice lifecycle scope is required"
        );
    }

    private static InvoiceLifecycleEligibilityDecision decision(
            Actor actor,
            PaymentBatchEligibility eligibility
    ) {
        MatrixCell cell = new MatrixCell(
                eligibility.batchStatus(),
                eligibility.batchTransactionStatus()
        );
        Set<MatrixCell> allowedCells = actor == Actor.MERCHANT
                ? MERCHANT_ALLOWED_CELLS
                : POINT_OF_SALE_ALLOWED_CELLS;
        return allowedCells.contains(cell)
                ? InvoiceLifecycleEligibilityDecision.ALLOWED
                : InvoiceLifecycleEligibilityDecision.DENIED;
    }

    private enum Actor {
        POINT_OF_SALE,
        MERCHANT
    }

    private record MatrixCell(
            RewardBatchStatus batchStatus,
            RewardBatchTrxStatus batchTransactionStatus
    ) {
    }
}
