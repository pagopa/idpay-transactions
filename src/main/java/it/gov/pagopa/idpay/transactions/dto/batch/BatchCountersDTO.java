package it.gov.pagopa.idpay.transactions.dto.batch;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BatchCountersDTO {
    private Long totalApprovedAmountCents;
    private Long trxElaborated;
    private Long trxSuspended;
    private Long trxRejected;

    public BatchCountersDTO incrementTotalApprovedAmountCents(Long amountCents){
        this.totalApprovedAmountCents = this.totalApprovedAmountCents + amountCents;
        return this;
    }

    public BatchCountersDTO decrementTotalApprovedAmountCents(Long amountCents){
        this.totalApprovedAmountCents = this.totalApprovedAmountCents - amountCents;
        return this;
    }

    public BatchCountersDTO incrementTrxElaborated(){
        this.trxElaborated = this.trxElaborated + 1L;
        return this;
    }

    public BatchCountersDTO decrementTrxElaborated(){
        this.trxElaborated = this.trxElaborated -1L;
        return this;
    }

    public BatchCountersDTO incrementTrxSuspended(){
        this.trxSuspended = this.trxSuspended + 1L;
        return this;
    }

    public BatchCountersDTO decrementTrxSuspended(){
        this.trxSuspended = this.trxSuspended -1L;
        return this;
    }

    public BatchCountersDTO incrementTrxRejected(){
        this.trxRejected = this.trxRejected + 1L;
        return this;
    }

    public BatchCountersDTO decrementTrxRejected(){
        this.trxRejected = this.trxRejected -1L;
        return this;
    }
}
