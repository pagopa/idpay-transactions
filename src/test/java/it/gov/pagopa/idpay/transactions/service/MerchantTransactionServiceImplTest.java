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
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
    void getMerchantTransactionList_withFiscalCode() {
        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward())
                .trxDate(LocalDateTime.now())
                .build();

        MerchantTransactionDTO expectedDto = MerchantTransactionDTO.builder()
                .trxId(rt1.getId())
                .effectiveAmountCents(rt1.getAmountCents())
                .rewardAmountCents(rt1.getRewards().get(INITIATIVE_ID).getAccruedRewardCents())
                .fiscalCode(FISCAL_CODE)
                .status(rt1.getStatus())
                .elaborationDateTime(rt1.getElaborationDateTime())
                .trxDate(rt1.getTrxDate())
                .channel(rt1.getChannel())
                .build();

        MerchantTransactionsListDTO expectedList = MerchantTransactionsListDTO.builder()
                .content(List.of(expectedDto))
                .pageNo(0)
                .pageSize(10)
                .totalElements(1)
                .totalPages(1)
                .build();

        FiscalCodeInfoPDV fiscalCodeInfo = new FiscalCodeInfoPDV(USER_ID);
        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        when(userRestClient.retrieveFiscalCodeInfo(anyString()))
                .thenReturn(Mono.just(fiscalCodeInfo));

        when(rewardTransactionRepository.findByFilter(
                any(),
                eq(USER_ID),
                eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepository.getCount(
                any(),
                isNull(),
                isNull(),
                eq(USER_ID)))
                .thenReturn(Mono.just(1L));

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        FISCAL_CODE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();
        assertNotNull(result);
        assertEquals(expectedList, result);

        verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);
        verify(rewardTransactionRepository).findByFilter(any(), eq(USER_ID),  eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), eq(USER_ID));
        verifyNoMoreInteractions(rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionList_noFiscalCode() {
        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward())
                .trxDate(LocalDateTime.now())
                .build();

        MerchantTransactionDTO expectedDto = MerchantTransactionDTO.builder()
                .trxId(rt1.getId())
                .effectiveAmountCents(rt1.getAmountCents())
                .rewardAmountCents(rt1.getRewards().get(INITIATIVE_ID).getAccruedRewardCents())
                .fiscalCode(FISCAL_CODE)
                .status(rt1.getStatus())
                .elaborationDateTime(rt1.getElaborationDateTime())
                .trxDate(rt1.getTrxDate())
                .channel(rt1.getChannel())
                .build();

        MerchantTransactionsListDTO expectedList = MerchantTransactionsListDTO.builder()
                .content(List.of(expectedDto))
                .pageNo(0)
                .pageSize(10)
                .totalElements(1)
                .totalPages(1)
                .build();

        UserInfoPDV userInfoPDV = new UserInfoPDV(FISCAL_CODE);
        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        when(rewardTransactionRepository.findByFilter(
                any(),
                isNull(),
                eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepository.getCount(
                any(),
                isNull(),
                isNull(),
                isNull()))
                .thenReturn(Mono.just(1L));

        when(userRestClient.retrieveUserInfo(anyString()))
                .thenReturn(Mono.just(userInfoPDV));

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();
        assertNotNull(result);
        assertEquals(expectedList, result);

        verify(rewardTransactionRepository).findByFilter(any(), isNull(), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), isNull());
        verify(userRestClient, atLeastOnce()).retrieveUserInfo(anyString());
    }

    @Test
    void getMerchantTransactions_shouldThrowForbiddenForMerchantToCheck() {
        Pageable paging = PageRequest.of(0, 10);

        ResponseStatusException ex = assertThrows(
                ResponseStatusException.class,
                () -> merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        "TO_CHECK",
                        null,
                        null,
                        paging
                )
        );

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        assertEquals("403 FORBIDDEN \"Status TO_CHECK not allowed for merchants\"", ex.getMessage());

        verifyNoInteractions(rewardTransactionRepository, userRestClient);
    }

    @Test
    void getMerchantTransactions_shouldThrowBadRequestForInvalidBatchStatus() {
        Pageable paging = PageRequest.of(0, 10);

        ResponseStatusException ex = assertThrows(
                ResponseStatusException.class,
                () -> merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        "WRONG_STATUS",
                        null,
                        null,
                        paging
                )
        );

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatusCode());
        assertEquals(
                "400 BAD_REQUEST \"Invalid rewardBatchTrxStatus value: WRONG_STATUS\"",
                ex.getMessage()
        );

        verifyNoInteractions(rewardTransactionRepository, userRestClient);
    }

    private static Map<String, Reward> getReward() {
        Map<String, Reward> reward = new HashMap<>();
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
        reward.put(INITIATIVE_ID, rewardElement);
        return reward;
    }

}
