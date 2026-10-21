package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;

public record TransactionNotes(ReasonDTO internal, ReasonDTO official) {
}
