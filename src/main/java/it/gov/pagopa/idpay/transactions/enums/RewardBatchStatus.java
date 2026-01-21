package it.gov.pagopa.idpay.transactions.enums;

public enum RewardBatchStatus {
  CREATED,
  SENT,
  EVALUATING,
  APPROVED,
  APPROVING,
  TO_APPROVE, //virtual state. Corresponds to EVALUATING + L3
  TO_WORK //virtual state. Corresponds to EVALUATING + L1, L2

}
