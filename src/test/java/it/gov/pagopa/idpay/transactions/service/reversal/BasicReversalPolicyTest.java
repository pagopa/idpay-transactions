package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class BasicReversalPolicyTest {

  private final BasicReversalPolicy policy = new BasicReversalPolicy();

  @Test
  void allowsOnlyInvoiced() {
    // TODO: check this test after using policy
    RewardTransaction invoiced = RewardTransaction.builder().status(SyncTrxStatus.INVOICED.name()).build();
    StepVerifier.create(policy.validate(invoiced)).verifyComplete();

    RewardTransaction rewarded = RewardTransaction.builder().status(SyncTrxStatus.REWARDED.name()).build();
    StepVerifier.create(policy.validate(rewarded))
        .expectError(ClientExceptionWithBody.class)
        .verify();
  }

  @Test
  void rejectsOtherStatuses() {
    RewardTransaction rt = RewardTransaction.builder().status(SyncTrxStatus.REFUNDED.name()).build();
    StepVerifier.create(policy.validate(rt)).expectError(ClientExceptionWithBody.class).verify();
  }
}
