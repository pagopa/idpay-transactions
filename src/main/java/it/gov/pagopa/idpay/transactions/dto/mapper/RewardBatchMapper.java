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
        .businessName(rewardBatch.getBusinessName())
        .month(rewardBatch.getMonth())
        .posType(rewardBatch.getPosType())
        .name(rewardBatch.getName())
        .status(rewardBatch.getStatus().toString())
        .partial(rewardBatch.getPartial())
        .startDate(rewardBatch.getStartDate())
        .endDate(rewardBatch.getEndDate())
        .approvedAmountCents(rewardBatch.getApprovedAmountCents())
        .suspendedAmountCents(rewardBatch.getSuspendedAmountCents() == null ? 0L : rewardBatch.getSuspendedAmountCents())
        .initialAmountCents(rewardBatch.getInitialAmountCents())
        .numberOfTransactions(rewardBatch.getNumberOfTransactions())
        .numberOfTransactionsElaborated(rewardBatch.getNumberOfTransactionsElaborated())
        .numberOfTransactionsSuspended(rewardBatch.getNumberOfTransactionsSuspended())
        .numberOfTransactionsRejected(rewardBatch.getNumberOfTransactionsRejected())
        .reportPath(rewardBatch.getReportPath())
        .assigneeLevel(String.valueOf(rewardBatch.getAssigneeLevel()))
        .merchantSendDate(rewardBatch.getMerchantSendDate())
        .build();

    return Mono.just(dto);
  }

}
