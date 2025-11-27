package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
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
    void setUp(){
        merchantTransactionService = new MerchantTransactionServiceImpl(userRestClient, rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionList_withFiscalCode() {
        // given
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

        Pageable paging = PageRequest.of(0, 10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        FiscalCodeInfoPDV fiscalCodeInfo = new FiscalCodeInfoPDV(USER_ID);

        when(userRestClient.retrieveFiscalCodeInfo(anyString()))
                .thenReturn(Mono.just(fiscalCodeInfo));

        when(rewardTransactionRepository.findByFilter(
                any(),
                eq(USER_ID),
                eq(OrganizationRole.MERCHANT),
                eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepository.getCount(
                any(),
                isNull(),
                isNull(),
                eq(USER_ID),
                eq(OrganizationRole.MERCHANT)))
                .thenReturn(Mono.just(1L));

        // when
        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        OrganizationRole.MERCHANT,
                        INITIATIVE_ID,
                        FISCAL_CODE,   // fiscalCode presente
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();

        // then
        assertNotNull(result);
        assertEquals(0, result.getPageNo());
        assertEquals(10, result.getPageSize());
        assertEquals(1, result.getTotalElements());
        assertEquals(1, result.getTotalPages());
        assertNotNull(result.getContent());
        assertEquals(1, result.getContent().size());

        MerchantTransactionDTO dto = result.getContent().get(0);
        assertEquals(rt1.getId(), dto.getTrxId());
        assertEquals(rt1.getAmountCents(), dto.getEffectiveAmountCents());
        assertEquals(
                rt1.getRewards().get(INITIATIVE_ID).getAccruedRewardCents(),
                dto.getRewardAmountCents()
        );
        // con fiscalCode passato come filtro, viene impostato direttamente
        assertEquals(FISCAL_CODE, dto.getFiscalCode());
        assertEquals(rt1.getStatus(), dto.getStatus());
        assertEquals(rt1.getElaborationDateTime(), dto.getElaborationDateTime());
        assertEquals(rt1.getTrxDate(), dto.getTrxDate());
        assertEquals(rt1.getChannel(), dto.getChannel());
        assertEquals(rt1.getTrxChargeDate(), dto.getTrxChargeDate());
        assertEquals(rt1.getAdditionalProperties(), dto.getAdditionalProperties());
        assertEquals(rt1.getTrxCode(), dto.getTrxCode());
        assertEquals(rt1.getRewardBatchTrxStatus(), dto.getRewardBatchTrxStatus());
        assertEquals(rt1.getPointOfSaleId(), dto.getPointOfSaleId());
        // authorizedAmountCents = importo - reward
        assertEquals(
                rt1.getAmountCents() - rt1.getRewards().get(INITIATIVE_ID).getAccruedRewardCents(),
                dto.getAuthorizedAmountCents()
        );

        verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);
        verify(userRestClient, never()).retrieveUserInfo(anyString());
        verify(rewardTransactionRepository).findByFilter(any(), eq(USER_ID), eq(OrganizationRole.MERCHANT), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), eq(USER_ID), eq(OrganizationRole.MERCHANT));
        verifyNoMoreInteractions(rewardTransactionRepository);
    }

    @Test
    void getMerchantTransactionList_noFiscalCode() {
        // given
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

        Pageable paging = PageRequest.of(0, 10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        // in assenza di fiscalCode il service chiama retrieveUserInfo(userId)
        UserInfoPDV userInfoPDV = new UserInfoPDV(FISCAL_CODE);

        when(rewardTransactionRepository.findByFilter(
                any(),
                isNull(),
                eq(OrganizationRole.MERCHANT),
                eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepository.getCount(
                any(),
                isNull(),
                isNull(),
                isNull(),
                eq(OrganizationRole.MERCHANT)))
                .thenReturn(Mono.just(1L));

        when(userRestClient.retrieveUserInfo(USER_ID))
                .thenReturn(Mono.just(userInfoPDV));

        // when
        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        OrganizationRole.MERCHANT,
                        INITIATIVE_ID,
                        null,  // fiscalCode assente
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();

        // then
        assertNotNull(result);
        assertEquals(0, result.getPageNo());
        assertEquals(10, result.getPageSize());
        assertEquals(1, result.getTotalElements());
        assertEquals(1, result.getTotalPages());
        assertNotNull(result.getContent());
        assertEquals(1, result.getContent().size());

        MerchantTransactionDTO dto = result.getContent().get(0);
        assertEquals(rt1.getId(), dto.getTrxId());
        assertEquals(rt1.getAmountCents(), dto.getEffectiveAmountCents());
        assertEquals(
                rt1.getRewards().get(INITIATIVE_ID).getAccruedRewardCents(),
                dto.getRewardAmountCents()
        );
        // qui il fiscalCode arriva da PDV (UserInfoPDV.pii)
        assertEquals(FISCAL_CODE, dto.getFiscalCode());
        assertEquals(rt1.getStatus(), dto.getStatus());
        assertEquals(rt1.getElaborationDateTime(), dto.getElaborationDateTime());
        assertEquals(rt1.getTrxDate(), dto.getTrxDate());
        assertEquals(rt1.getChannel(), dto.getChannel());
        assertEquals(rt1.getTrxChargeDate(), dto.getTrxChargeDate());
        assertEquals(rt1.getAdditionalProperties(), dto.getAdditionalProperties());
        assertEquals(rt1.getTrxCode(), dto.getTrxCode());
        assertEquals(rt1.getRewardBatchTrxStatus(), dto.getRewardBatchTrxStatus());
        assertEquals(rt1.getPointOfSaleId(), dto.getPointOfSaleId());
        assertEquals(
                rt1.getAmountCents() - rt1.getRewards().get(INITIATIVE_ID).getAccruedRewardCents(),
                dto.getAuthorizedAmountCents()
        );

        verify(rewardTransactionRepository).findByFilter(any(), isNull(), eq(OrganizationRole.MERCHANT), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), isNull(), eq(OrganizationRole.MERCHANT));
        verify(userRestClient).retrieveUserInfo(USER_ID);
        verify(userRestClient, never()).retrieveFiscalCodeInfo(anyString());
    }

    @Test
    void getMerchantTransactions_shouldThrowForbiddenForMerchantToCheck() {
        Pageable paging = PageRequest.of(0, 10);

        ResponseStatusException ex = assertThrows(
                ResponseStatusException.class,
                () -> merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        OrganizationRole.MERCHANT,
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        "TO_CHECK",
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
                        OrganizationRole.MERCHANT,
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
