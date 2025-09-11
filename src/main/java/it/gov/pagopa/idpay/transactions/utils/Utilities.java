package it.gov.pagopa.idpay.transactions.utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utilities {

  private Utilities() {}

  public static String sanitizeString(String str){
    return str.replaceAll("[\\r\\n]", "").replaceAll("[^\\w\\s-]", "");
  }
}
