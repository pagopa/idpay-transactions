package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class MerchantTransactionServiceImplTest {

    @Mock
    private RewardTransactionRepository rewardTransactionRepository;
    @Mock
    private UserRestClient userRestClient;

    private MerchantTransactionService merchantTransactionService;

    private static final String INITIATIVE_ID = "INITIATIVEID1";
    private static final String MERCHANT_ID = "MERCHANTID1";
    private static final String USER_ID = "USERID1";
    private static final String FISCAL_CODE = "FISCALCODE1";

    @BeforeEach
    void setUp() {
        merchantTransactionService =
                new MerchantTransactionServiceImpl(userRestClient, rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionListWithFiscalCode() {
        RewardTransaction rewardTransaction = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward())
                .build();

        MerchantTransactionDTO merchantTransaction = MerchantTransactionDTO.builder()
                .trxId(rewardTransaction.getId())
                .effectiveAmountCents(rewardTransaction.getAmountCents())
                .rewardAmountCents(rewardTransaction.getRewards().get(INITIATIVE_ID).getAccruedRewardCents())
                .fiscalCode(FISCAL_CODE)
                .status(rewardTransaction.getStatus())
                .elaborationDateTime(rewardTransaction.getElaborationDateTime())
                .trxDate(rewardTransaction.getTrxDate())
                .channel(rewardTransaction.getChannel())
                .build();

        MerchantTransactionsListDTO merchantTransactionsListExpected = MerchantTransactionsListDTO.builder()
                .content(List.of(merchantTransaction))
                .pageSize(10)
                .totalElements(1)
                .totalPages(1)
                .build();

        FiscalCodeInfoPDV fiscalCodeInfo = new FiscalCodeInfoPDV(USER_ID);
        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        Mockito.when(userRestClient.retrieveFiscalCodeInfo(anyString()))
                .thenReturn(Mono.just(fiscalCodeInfo));

        Mockito.when(rewardTransactionRepository.findByFilter(
                        anyString(), anyString(), anyString(),
                        any(), any(), any(), any(Pageable.class)))
                .thenReturn(Flux.just(rewardTransaction));

        Mockito.when(rewardTransactionRepository.getCount(
                        anyString(), anyString(), any(), any(), any(), any()))
                .thenReturn(Mono.just(1L));

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        FISCAL_CODE,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();

        assertEquals(merchantTransactionsListExpected, result);

        Mockito.verify(userRestClient, never()).retrieveUserInfo(anyString());
    }

    @Test
    void getMerchantTransactionListWithoutFiscalCode() {
        RewardTransaction rewardTransaction = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward())
                .build();

        MerchantTransactionDTO merchantTransaction = MerchantTransactionDTO.builder()
                .trxId(rewardTransaction.getId())
                .effectiveAmountCents(rewardTransaction.getAmountCents())
                .rewardAmountCents(rewardTransaction.getRewards().get(INITIATIVE_ID).getAccruedRewardCents())
                .fiscalCode(FISCAL_CODE)
                .status(rewardTransaction.getStatus())
                .elaborationDateTime(rewardTransaction.getElaborationDateTime())
                .trxDate(rewardTransaction.getTrxDate())
                .channel(rewardTransaction.getChannel())
                .build();

        MerchantTransactionsListDTO merchantTransactionsListExpected = MerchantTransactionsListDTO.builder()
                .content(List.of(merchantTransaction))
                .pageSize(10)
                .totalElements(1)
                .totalPages(1)
                .build();

        UserInfoPDV userInfo = new UserInfoPDV(FISCAL_CODE);
        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        Mockito.when(userRestClient.retrieveUserInfo(anyString()))
                .thenReturn(Mono.just(userInfo));

        Mockito.when(rewardTransactionRepository.findByFilter(
                        anyString(), anyString(), isNull(),
                        any(), any(), any(), any(Pageable.class)))
                .thenReturn(Flux.just(rewardTransaction));

        Mockito.when(rewardTransactionRepository.getCount(
                        anyString(), anyString(), any(), any(), isNull(), any()))
                .thenReturn(Mono.just(1L));

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();

        assertEquals(merchantTransactionsListExpected, result);
    }

    private static Map<String, Reward> getReward() {
        Map<String, Reward> rewardMap = new HashMap<>();
        RewardCounters counter = RewardCounters.builder()
                .exhaustedBudget(false)
                .initiativeBudgetCents(10000L)
                .build();
        Reward rewardElement = Reward.builder()
                .initiativeId(INITIATIVE_ID)
                .organizationId("ORGANIZATIONID")
                .providedRewardCents(1000L)
                .accruedRewardCents(1000L)
                .capped(false)
                .dailyCapped(false)
                .monthlyCapped(false)
                .yearlyCapped(false)
                .weeklyCapped(false)
                .counters(counter)
                .build();
        rewardMap.put(INITIATIVE_ID, rewardElement);
        return rewardMap;
    }
}
