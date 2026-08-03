package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.PaymentRewardBatchImpactDTO;
import it.gov.pagopa.idpay.transactions.model.PaymentRewardBatchImpact;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PaymentRewardBatchImpactMapper {

    private final RewardTransactionMapper rewardTransactionMapper;

    public PaymentRewardBatchImpact mapFromDTO(PaymentRewardBatchImpactDTO impact) {
        if (impact == null) {
            throw new IllegalArgumentException("Payment reward batch impact is required");
        }
        return new PaymentRewardBatchImpact(
                impact.getEventId(),
                impact.getSchemaVersion() == null ? 0 : impact.getSchemaVersion(),
                impact.getImpactType(),
                impact.getOccurredAt(),
                impact.getTransactionRevision() == null ? 0L : impact.getTransactionRevision(),
                rewardTransactionMapper.mapFromDTO(impact.getTransaction())
        );
    }
}
