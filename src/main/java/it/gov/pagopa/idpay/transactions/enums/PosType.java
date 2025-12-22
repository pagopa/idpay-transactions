package it.gov.pagopa.idpay.transactions.enums;

import lombok.Getter;

@Getter
public enum PosType {

  PHYSICAL("Fisico"),
  ONLINE("Online");

  private final String description;

  PosType(String description) {
    this.description = description;
  }

  public static PosType fromDescription(String description) {
    if (description == null) {
      return null;
    }
    for (PosType posType : PosType.values()) {
      if (posType.description.equalsIgnoreCase(description.trim())) {
        return posType;
      }
    }
    return null;
  }
}
