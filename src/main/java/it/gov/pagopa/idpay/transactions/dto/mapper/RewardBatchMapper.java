package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class RewardBatchMapper {

  public Mono<RewardBatchDTO> toDTO(RewardBatch rewardBatch) {

    RewardBatchDTO dto = RewardBatchDTO.builder()
        .id(rewardBatch.getId())
        .merchantId(rewardBatch.getMerchantId())
        .month(rewardBatch.getMonth())
        .posType(rewardBatch.getPosType().toString())
        .name(rewardBatch.getName())
        .status(rewardBatch.getStatus().toString())
        .batchType(rewardBatch.getBatchType().toString())
        .partial(rewardBatch.getPartial())
        .startDate(rewardBatch.getStartDate())
        .endDate(rewardBatch.getEndDate())
        .totalAmountCents(rewardBatch.getTotalAmountCents())
        .build();

    return Mono.just(dto);
  }

}
