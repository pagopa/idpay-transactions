package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@Slf4j
public class MerchantRewardBatchControllerImpl implements MerchantRewardBatchController{

    private final RewardBatchService rewardBatchService;
    private final RewardBatchMapper rewardBatchMapper;

    public MerchantRewardBatchControllerImpl(RewardBatchService rewardBatchService, RewardBatchMapper rewardBatchMapper){
        this.rewardBatchService = rewardBatchService;
        this.rewardBatchMapper = rewardBatchMapper;
    }

    @Override
    public Mono<RewardBatchListDTO> getMerchantRewardBatches(String merchantId, String initiativeId, Pageable pageable) {
        if(merchantId!=null) {
            log.info("[GET_MERCHANT_REWARD_BATCHES] Merchant {} requested to retrieve reward batches", Utilities.sanitizeString(merchantId));
            return this.rewardBatchService.getMerchantRewardBatches(merchantId, pageable)
                    .flatMap(page ->
                            Flux.fromIterable(page.getContent())
                                    .flatMapSequential(rewardBatchMapper::toDTO)
                                    .collectList()
                                    .map(dtoList -> new RewardBatchListDTO(
                                            dtoList,
                                            page.getNumber(),
                                            page.getSize(),
                                            (int) page.getTotalElements(),
                                            page.getTotalPages()))
                    );
        }else{
            log.info("[GET_ALL_REWARD_BATCHES] Received a request to retrieve all reward batches");
            return this.rewardBatchService.getAllRewardBatches(pageable)
                    .flatMap(page ->
                            Flux.fromIterable(page.getContent())
                                    .flatMapSequential(rewardBatchMapper::toDTO)
                                    .collectList()
                                    .map(dtoList -> new RewardBatchListDTO(
                                            dtoList,
                                            page.getNumber(),
                                            page.getSize(),
                                            (int) page.getTotalElements(),
                                            page.getTotalPages()))
                    );
        }
    }

    @Override
    public Mono<Void> sendRewardBatches(String merchantId, String initiativeId, String batchId) {
        log.info("[SEND_REWARD_BATCHES] Merchant {} requested to send batch batchId {}",
                Utilities.sanitizeString(merchantId), Utilities.sanitizeString(batchId));
        this.rewardBatchService.sendRewardBatch(merchantId, batchId);
        return Mono.empty();
    }
}