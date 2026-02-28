package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class BasicReversalPolicyTest {

  private final BasicReversalPolicy policy = new BasicReversalPolicy();

  @Test
  void allowsInvoiced() {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus("INVOICED");

    StepVerifier.create(policy.validate(rt)).verifyComplete();
  }

  @Test
  void rejectsRewarded() {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus("REWARDED");

    StepVerifier.create(policy.validate(rt)).expectError().verify();
  }

  @Test
  void rejectsOtherStatuses() {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus("REFUNDED");

    StepVerifier.create(policy.validate(rt)).expectError().verify();
  }
}
