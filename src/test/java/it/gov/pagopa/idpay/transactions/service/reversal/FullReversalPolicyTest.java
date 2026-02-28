package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class FullReversalPolicyTest {

  private final FullReversalPolicy policy = new FullReversalPolicy();

  @Test
  void allowsInvoiced() {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus("INVOICED");

    StepVerifier.create(policy.validate(rt)).verifyComplete();
  }

  @Test
  void allowsRewarded() {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus("REWARDED");

    StepVerifier.create(policy.validate(rt)).verifyComplete();
  }

  @Test
  void rejectsOtherStatuses() {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus("REFUNDED");

    StepVerifier.create(policy.validate(rt)).expectError().verify();
  }
}
