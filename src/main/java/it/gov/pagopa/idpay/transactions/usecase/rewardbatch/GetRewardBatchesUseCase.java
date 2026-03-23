package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@AllArgsConstructor
public class GetRewardBatchesUseCase {

    private final RewardBatchRepository rewardBatchRepository;

    public Mono<Page<RewardBatch>> execute(String merchantId, String organizationRole, String status, String assigneeLevel, String month, Pageable pageable) {
        boolean callerIsOperator = RewardBatchSharedUtils.isOperator(organizationRole);

        return rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, month, callerIsOperator, pageable)
                .collectList()
                .zipWith(rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, month, callerIsOperator))
                .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
    }
}

