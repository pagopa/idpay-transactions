package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@Slf4j
public class RewardTransactionServiceImpl implements RewardTransactionService {

    private final RewardTransactionRepository rewardTrxRepository;
    private final RewardTransactionMapper mapper;
    private final ErrorNotifierService errorNotifierService;

    private final ObjectReader objectReader;

    public RewardTransactionServiceImpl(RewardTransactionRepository rewardTrxRepository, RewardTransactionMapper mapper, ErrorNotifierService errorNotifierService, ObjectMapper objectMapper) {
        this.rewardTrxRepository = rewardTrxRepository;
        this.mapper = mapper;
        this.errorNotifierService = errorNotifierService;
        this.objectReader = objectMapper.readerFor(RewardTransactionDTO.class);
    }

    public Flux<RewardTransaction> save(Flux<Message<String>> messageFlux) {
        return messageFlux.flatMap(this::save);
    }

    public Mono<RewardTransaction> save(Message<String> message) {
        return Mono.just(message)
                .mapNotNull(this::deserializeMessage)
                .map(this.mapper::mapFromDTO)
                .flatMap(this.rewardTrxRepository::save)

                .onErrorResume(e -> {
                    errorNotifierService.notifyTransaction(message, "An error occurred while storing transaction", true, e);
                    return Mono.empty();
                });
    }

    private RewardTransactionDTO deserializeMessage(Message<String> message) {
        return Utils.deserializeMessage(message, objectReader, e -> errorNotifierService.notifyTransaction(message, "Unexpected JSON", true, e));
    }

    public Flux<RewardTransaction> findAll(String startDate, String endDate, String userId, String hpan, String acquirerId){
        if(startDate==null || endDate == null){
            return null;
        }else {
            return rewardTrxRepository.findAllInRange(LocalDateTime.parse(startDate, DateTimeFormatter.ISO_DATE_TIME), LocalDateTime.parse(endDate, DateTimeFormatter.ISO_DATE_TIME))
                    .filter(transaction ->
                            checkEqualObject(transaction.getUserId(), userId)
                            && checkEqualObject(transaction.getHpan(), hpan)
                            && checkEqualObject(transaction.getAcquirerId(), acquirerId))
                    .onErrorResume(e -> {log.error("Error occurred in searching transactions",e);
                        return Flux.empty();});
        }
    }

    private static <T> boolean checkEqualObject(T value, T expected){
        if(expected != null){
            return value.equals(expected);
        } else {
            return true;
        }
    }
}
