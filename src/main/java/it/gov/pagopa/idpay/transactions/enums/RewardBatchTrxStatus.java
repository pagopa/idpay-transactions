package it.gov.pagopa.idpay.transactions.enums;

import lombok.Getter;

@Getter
public enum RewardBatchTrxStatus {
  TO_CHECK("Da esaminare"),
  CONSULTABLE("Consultabile"),
  SUSPENDED("Da controllare"),
  APPROVED("Approvata"),
  REJECTED("Esclusa");
  //UNDEFINED("Indefinito"); //status tecnico per cred. deboli per il null

  private final String description;

  RewardBatchTrxStatus(String description) {
    this.description = description;
  }

  public static RewardBatchTrxStatus fromDescription(String description) {
    if (description == null) {
      return null;
    }
    for (RewardBatchTrxStatus status : RewardBatchTrxStatus.values()) {
      if (status.description.equalsIgnoreCase(description.trim())) {
        return status;
      }
    }
    return null;
  }
}

