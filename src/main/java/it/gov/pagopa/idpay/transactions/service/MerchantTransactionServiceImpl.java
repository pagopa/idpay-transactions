package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class MerchantTransactionServiceImpl implements MerchantTransactionService {
    private final UserRestClient userRestClient;
    private final RewardTransactionRepository rewardTransactionRepository;
    private final ChecksErrorMapper checksErrorMapper;
    private static final Set<String> OPERATORS =
            Set.of("operator1", "operator2", "operator3");

    protected MerchantTransactionServiceImpl(
            UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository,
            ChecksErrorMapper checksErrorMapper) {
        this.userRestClient = userRestClient;
        this.rewardTransactionRepository = rewardTransactionRepository;
        this.checksErrorMapper = checksErrorMapper;
    }


    @Override
    public Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole) {

        List<String> allStatuses = Arrays.stream(RewardBatchTrxStatus.values())
                .map(Enum::name)
                .toList();

        if (isOperator(organizationRole)) {
            return Mono.just(allStatuses);
        } else {
            return Mono.just(
                    allStatuses.stream()
                            .filter(s -> !"TO_CHECK".equalsIgnoreCase(s))
                            .toList()
            );
        }
    }




    private boolean isOperator(String role) {
        return role != null && OPERATORS.contains(role.toLowerCase());
    }
}
