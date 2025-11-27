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
    void setUp() {
        merchantTransactionService =
                new MerchantTransactionServiceImpl(userRestClient, rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionList_withFiscalCode() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(now)
                .rewards(getReward())
                .trxDate(now)
                .build();

        Pageable paging = PageRequest.of(
                0,
                10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending()
        );

        FiscalCodeInfoPDV fiscalCodeInfo = new FiscalCodeInfoPDV(USER_ID);

        when(userRestClient.retrieveFiscalCodeInfo(anyString()))
                .thenReturn(Mono.just(fiscalCodeInfo));

        when(rewardTransactionRepository.findByFilter(any(), eq(USER_ID), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepository.getCount(any(), isNull(), isNull(), eq(USER_ID)))
                .thenReturn(Mono.just(1L));

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        FISCAL_CODE,
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();

        assertPage(result, 0, 10, 1, 1);

        MerchantTransactionDTO dto = result.getContent().get(0);
        assertMerchantTransactionMatches(rt1, dto, FISCAL_CODE);

        verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);
        verify(userRestClient, never()).retrieveUserInfo(anyString());
        verify(rewardTransactionRepository).findByFilter(any(), eq(USER_ID), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), eq(USER_ID));
        verifyNoMoreInteractions(rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionList_noFiscalCode() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(now)
                .rewards(getReward())
                .trxDate(now)
                .build();

        Pageable paging = PageRequest.of(
                0,
                10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending()
        );

        UserInfoPDV userInfoPDV = new UserInfoPDV(FISCAL_CODE);

        when(rewardTransactionRepository.findByFilter(any(), isNull(), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepository.getCount(any(), isNull(), isNull(), isNull()))
                .thenReturn(Mono.just(1L));

        when(userRestClient.retrieveUserInfo(USER_ID))
                .thenReturn(Mono.just(userInfoPDV));


        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();

        assertPage(result, 0, 10, 1, 1);

        MerchantTransactionDTO dto = result.getContent().get(0);
        assertMerchantTransactionMatches(rt1, dto, FISCAL_CODE);

        verify(rewardTransactionRepository).findByFilter(any(), isNull(), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), isNull());
        verify(userRestClient).retrieveUserInfo(USER_ID);
        verify(userRestClient, never()).retrieveFiscalCodeInfo(anyString());
    }

    @Test
    void getMerchantTransactions_shouldThrowBadRequestForInvalidBatchStatus() {
        Pageable paging = PageRequest.of(0, 10);

        ResponseStatusException ex = assertThrows(
                ResponseStatusException.class,
                () -> merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        "WRONG_STATUS",
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

    private void assertPage(MerchantTransactionsListDTO result,
                            int pageNo,
                            int pageSize,
                            int totalElements,
                            int totalPages) {
        assertNotNull(result);
        assertEquals(pageNo, result.getPageNo());
        assertEquals(pageSize, result.getPageSize());
        assertEquals(totalElements, result.getTotalElements());
        assertEquals(totalPages, result.getTotalPages());
        assertNotNull(result.getContent());
        assertEquals(totalElements, result.getContent().size());
    }

    private void assertMerchantTransactionMatches(RewardTransaction expected,
                                                  MerchantTransactionDTO actual,
                                                  String expectedFiscalCode) {
        assertEquals(expected.getId(), actual.getTrxId());
        assertEquals(expected.getAmountCents(), actual.getEffectiveAmountCents());
        assertEquals(
                expected.getRewards().get(INITIATIVE_ID).getAccruedRewardCents(),
                actual.getRewardAmountCents()
        );
        assertEquals(expectedFiscalCode, actual.getFiscalCode());
        assertEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected.getElaborationDateTime(), actual.getElaborationDateTime());
        assertEquals(expected.getTrxDate(), actual.getTrxDate());
        assertEquals(expected.getChannel(), actual.getChannel());
        assertEquals(expected.getTrxChargeDate(), actual.getTrxChargeDate());
        assertEquals(expected.getAdditionalProperties(), actual.getAdditionalProperties());
        assertEquals(expected.getTrxCode(), actual.getTrxCode());
        assertEquals(expected.getRewardBatchTrxStatus(), actual.getRewardBatchTrxStatus());
        assertEquals(expected.getPointOfSaleId(), actual.getPointOfSaleId());
        assertEquals(
                expected.getAmountCents()
                        - expected.getRewards().get(INITIATIVE_ID).getAccruedRewardCents(),
                actual.getAuthorizedAmountCents()
        );
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
