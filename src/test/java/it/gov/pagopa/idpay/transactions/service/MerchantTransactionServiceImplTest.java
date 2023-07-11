package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.utils.CommonUtilities;
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
import org.jetbrains.annotations.NotNull;
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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

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
    void setUp(){
        merchantTransactionService = new MerchantTransactionServiceImpl(userRestClient, rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionList() {
        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward()).build();

        MerchantTransactionDTO merchantTransaction1 = MerchantTransactionDTO.builder()
                .trxId(rt1.getId())
                .effectiveAmount(rt1.getAmountCents())
                .rewardAmount(CommonUtilities.euroToCents(rt1.getRewards().get(INITIATIVE_ID).getProvidedReward()))
                .fiscalCode(FISCAL_CODE)
                .status(rt1.getStatus())
                .elaborationDateTime(rt1.getElaborationDateTime())
                .trxDate(rt1.getTrxDate())
                .build();

        MerchantTransactionsListDTO merchantTransactionsListDTO_expected = MerchantTransactionsListDTO.builder()
                .content(List.of(merchantTransaction1))
                .pageSize(10).totalElements(1).totalPages(1).build();

        UserInfoPDV userId = new UserInfoPDV(FISCAL_CODE);
        FiscalCodeInfoPDV fiscalCode = new FiscalCodeInfoPDV(USER_ID);
        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        Mockito.when(userRestClient.retrieveFiscalCodeInfo(anyString())).thenReturn(Mono.just(fiscalCode));
        Mockito.when(rewardTransactionRepository.findByFilter(MERCHANT_ID, INITIATIVE_ID, USER_ID, null, paging)).thenReturn(Flux.just(rt1));
        Mockito.when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, USER_ID, null)).thenReturn(Mono.just(1L));

        Mono<MerchantTransactionsListDTO> resultMono = merchantTransactionService.getMerchantTransactions(MERCHANT_ID, INITIATIVE_ID, FISCAL_CODE, null, paging);

        MerchantTransactionsListDTO result = resultMono.block();

        assertEquals(merchantTransactionsListDTO_expected, result);
    }

    @Test
    void getMerchantTransactionListNoFiscalCode() {
        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward()).build();

        MerchantTransactionDTO merchantTransaction1 = MerchantTransactionDTO.builder()
                .trxId(rt1.getId())
                .effectiveAmount(rt1.getAmountCents())
                .rewardAmount(CommonUtilities.euroToCents(rt1.getRewards().get(INITIATIVE_ID).getProvidedReward()))
                .fiscalCode(FISCAL_CODE)
                .status(rt1.getStatus())
                .elaborationDateTime(rt1.getElaborationDateTime())
                .trxDate(rt1.getTrxDate())
                .build();

        MerchantTransactionsListDTO merchantTransactionsListDTO_expected = MerchantTransactionsListDTO.builder()
                .content(List.of(merchantTransaction1))
                .pageSize(10).totalElements(1).totalPages(1).build();

        UserInfoPDV userId = new UserInfoPDV(FISCAL_CODE);
        FiscalCodeInfoPDV fiscalCode = new FiscalCodeInfoPDV(USER_ID);
        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        Mockito.when(userRestClient.retrieveUserInfo(anyString())).thenReturn(Mono.just(userId));
        Mockito.when(rewardTransactionRepository.findByFilter(MERCHANT_ID, INITIATIVE_ID, null, null, paging)).thenReturn(Flux.just(rt1));
        Mockito.when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, null, null)).thenReturn(Mono.just(1L));

        Mono<MerchantTransactionsListDTO> resultMono = merchantTransactionService.getMerchantTransactions(MERCHANT_ID, INITIATIVE_ID, null, null, paging);

        MerchantTransactionsListDTO result = resultMono.block();

        assertEquals(merchantTransactionsListDTO_expected, result);
    }

    @NotNull
    private static Map<String, Reward> getReward() {
        Map<String, Reward> reward = new HashMap<>();
        RewardCounters counter = RewardCounters.builder()
                .exhaustedBudget(false)
                .initiativeBudget(new BigDecimal("100.00"))
                .build();
        Reward rewardElement = Reward.builder()
                .initiativeId(INITIATIVE_ID)
                .organizationId("ORGANIZATIONID")
                .providedReward(BigDecimal.TEN)
                .accruedReward(BigDecimal.TEN)
                .capped(false)
                .dailyCapped(false)
                .monthlyCapped(false)
                .yearlyCapped(false)
                .weeklyCapped(false)
                .counters(counter)
                .build();
        reward.put(INITIATIVE_ID, rewardElement);
        return reward;
    }

}
