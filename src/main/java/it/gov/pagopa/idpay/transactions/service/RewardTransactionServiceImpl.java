package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;

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

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount, Pageable pageable) {
        return rewardTrxRepository.findByIdTrxIssuer(idTrxIssuer, userId, trxDateStart, trxDateEnd, amount, pageable);
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount, Pageable pageable) {
        return rewardTrxRepository.findByRange(userId, trxDateStart, trxDateEnd, amount, pageable);
    }
}
