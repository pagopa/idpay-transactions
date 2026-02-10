package it.gov.pagopa.idpay.transactions.dto.batch;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class BatchCountersDTO {
  private Long initialAmountCents;
  private Long numberOfTransactions;
  private Long suspendedAmountCents;
  private Long trxSuspended;
  private Long approvedAmountCents;
  private Long trxElaborated;
  private Long trxRejected;

  private BatchCountersDTO() {
    this.initialAmountCents = 0L;
    this.numberOfTransactions = 0L;
    this.approvedAmountCents = 0L;
    this.suspendedAmountCents = 0L;
    this.trxElaborated = 0L;
    this.trxSuspended = 0L;
    this.trxRejected = 0L;
  }

  public static BatchCountersDTO newBatch() {
    return new BatchCountersDTO();
  }

  public BatchCountersDTO incrementInitialAmountCents(Long amountCents) {
    this.initialAmountCents = this.initialAmountCents + amountCents;
    return this;
  }

  public BatchCountersDTO decrementInitialAmountCents(Long amountCents) {
    this.initialAmountCents = this.initialAmountCents - amountCents;
    return this;
  }

  public BatchCountersDTO incrementNumberOfTransactions() {
    this.numberOfTransactions = this.numberOfTransactions + 1L;
    return this;
  }

  public BatchCountersDTO incrementNumberOfTransactions(Long number) {
    this.numberOfTransactions = this.numberOfTransactions + number;
    return this;
  }

  public BatchCountersDTO decrementNumberOfTransactions() {
    this.numberOfTransactions = this.numberOfTransactions - 1L;
    return this;
  }

  public BatchCountersDTO incrementApprovedAmountCents(Long amountCents) {
    this.approvedAmountCents = this.approvedAmountCents + amountCents;
    return this;
  }

  public BatchCountersDTO decrementApprovedAmountCents(Long amountCents) {
    this.approvedAmountCents = this.approvedAmountCents - amountCents;
    return this;
  }

  public BatchCountersDTO incrementSuspendedAmountCents(Long amountCents) {
    this.suspendedAmountCents = this.suspendedAmountCents + amountCents;
    return this;
  }

  public BatchCountersDTO decrementSuspendedAmountCents(Long amountCents) {
    this.suspendedAmountCents = this.suspendedAmountCents - amountCents;
    return this;
  }

  public BatchCountersDTO incrementTrxElaborated() {
    this.trxElaborated = this.trxElaborated + 1L;
    return this;
  }

  public BatchCountersDTO incrementTrxElaborated(Long number) {
    this.trxElaborated = this.trxElaborated + number;
    return this;
  }

  public BatchCountersDTO decrementTrxElaborated() {
    this.trxElaborated = this.trxElaborated - 1L;
    return this;
  }

  public BatchCountersDTO incrementTrxSuspended() {
    this.trxSuspended = this.trxSuspended + 1L;
    return this;
  }

  public BatchCountersDTO incrementTrxSuspended(Long number) {
    this.trxSuspended = this.trxSuspended + number;
    return this;
  }

  public BatchCountersDTO decrementTrxSuspended() {
    this.trxSuspended = this.trxSuspended - 1L;
    return this;
  }

  public BatchCountersDTO incrementTrxRejected(Long number) {
    this.trxRejected = this.trxRejected + number;
    return this;
  }

  public BatchCountersDTO incrementTrxRejected() {
    this.trxRejected = this.trxRejected + 1L;
    return this;
  }

  public BatchCountersDTO decrementTrxRejected() {
    this.trxRejected = this.trxRejected - 1L;
    return this;
  }
}
