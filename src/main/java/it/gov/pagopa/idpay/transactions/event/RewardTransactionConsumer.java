package it.gov.pagopa.idpay.transactions.event;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class RewardTransactionConsumer {

    @Autowired
    private RewardTransactionService rewardTransactionService;

    @Autowired
    private RewardTransactionMapper mapper;

    @Bean
    Consumer<Flux<RewardTransactionDTO>> rewardTrxConsumer(){
        return rewardTransactionDTOFlux -> rewardTransactionDTOFlux.map(this.mapper::mapFromDTO)
                .flatMap(this.rewardTransactionService::save)
                .subscribe(transaction -> log.info("Transaction save: {}", transaction));
    }
}
