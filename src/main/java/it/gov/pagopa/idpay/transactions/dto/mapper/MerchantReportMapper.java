package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.MerchantReportDTO;
import it.gov.pagopa.idpay.transactions.model.MerchantReport;
import org.springframework.stereotype.Component;

@Component
public class MerchantReportMapper {

    public MerchantReportDTO mapToDTO(MerchantReport entity) {
        return MerchantReportDTO.builder()
                .initiativeId(entity.getInitiativeId())
                .reportStatus(entity.getReportStatus())
                .startPeriod(entity.getStartPeriod())
                .endPeriod(entity.getEndPeriod())
                .merchantId(entity.getMerchantId())
                .businessName(entity.getBusinessName())
                .requestDate(entity.getRequestDate())
                .rewardBatchAssignee(entity.getRewardBatchAssignee())
                .fileName(entity.getFileName())
                .build();
    }
}
