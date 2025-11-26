package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MerchantTransactionServiceImplTest {

    @Mock
    private UserRestClient userRestClient;
    @Mock
    private RewardTransactionRepository rewardTransactionRepository;


    @InjectMocks
    private MerchantTransactionServiceImpl service;

    private RewardTransaction buildRewardTransaction(String id, String userId, String initiativeId) {
        RewardTransaction t = new RewardTransaction();
        t.setId(id);
        t.setUserId(userId);
        t.setAmountCents(100L);
        t.setTrxDate(LocalDateTime.now());
        t.setElaborationDateTime(LocalDateTime.now());
        t.setStatus("REWARDED");
        t.setChannel("ONLINE");

        Map<String, Reward> rewards = new HashMap<>();
        Reward r = new Reward();
        r.setAccruedRewardCents(50L);
        rewards.put(initiativeId, r);
        t.setRewards(rewards);

        return t;
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

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        OrganizationRole.MERCHANT,
                        INITIATIVE_ID,
                        FISCAL_CODE,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();
        assertNotNull(result);
        assertEquals(expectedList, result);

        verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);
        verify(rewardTransactionRepository).findByFilter(any(), eq(USER_ID), eq(OrganizationRole.MERCHANT), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), eq(USER_ID), eq(OrganizationRole.MERCHANT));
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

        when(userRestClient.retrieveUserInfo(anyString()))
                .thenReturn(Mono.just(userInfoPDV));

        Mono<MerchantTransactionsListDTO> resultMono =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        OrganizationRole.MERCHANT,
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        null,
                        paging
                );

        MerchantTransactionsListDTO result = resultMono.block();
        assertNotNull(result);
        assertEquals(expectedList, result);

        verify(rewardTransactionRepository).findByFilter(any(), isNull(), eq(OrganizationRole.MERCHANT), eq(paging));
        verify(rewardTransactionRepository).getCount(any(), isNull(), isNull(), isNull(), eq(OrganizationRole.MERCHANT));
        verify(userRestClient, atLeastOnce()).retrieveUserInfo(anyString());
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

    @Test
    void testCreateMerchantTransactionDTO_withFiscalCode() throws Exception {
        String initiativeId = "I1";

        RewardTransaction trx = buildRewardTransaction("T1", "USER1", initiativeId);

        var method = MerchantTransactionServiceImpl.class
                .getDeclaredMethod("createMerchantTransactionDTO",
                        String.class, RewardTransaction.class, String.class);
        method.setAccessible(true);

        Mono<MerchantTransactionDTO> result =
                (Mono<MerchantTransactionDTO>) method.invoke(service,
                        initiativeId, trx, "FISCALCODE123");

        StepVerifier.create(result)
                .assertNext(dto -> {
                    assert dto.getFiscalCode().equals("FISCALCODE123");
                })
                .verifyComplete();
    }

    @Test
    void testCreateMerchantTransactionDTO_withoutFiscalCode() throws Exception {
        String initiativeId = "I1";
        RewardTransaction trx = buildRewardTransaction("T1", "USERX", initiativeId);

        UserInfoPDV userInfo = new UserInfoPDV();
        userInfo.setPii("CF_FROM_PDV");

        when(userRestClient.retrieveUserInfo("USERX"))
                .thenReturn(Mono.just(userInfo));

        var method = MerchantTransactionServiceImpl.class
                .getDeclaredMethod("createMerchantTransactionDTO",
                        String.class, RewardTransaction.class, String.class);
        method.setAccessible(true);

        Mono<MerchantTransactionDTO> result =
                (Mono<MerchantTransactionDTO>) method.invoke(service,
                        initiativeId, trx, null);

        StepVerifier.create(result)
                .assertNext(dto -> {
                    assert dto.getFiscalCode().equals("CF_FROM_PDV");
                })
                .verifyComplete();
    }

    @Test
    void testGetMerchantTransactionDTOs_sortingAndCount() throws Exception {
        String initiativeId = "I1";
        Pageable pageable = PageRequest.of(0, 10);

        RewardTransaction t1 = buildRewardTransaction("T1", "U1", initiativeId);
        RewardTransaction t2 = buildRewardTransaction("T2", "U1", initiativeId);

        List<RewardTransaction> ordered = List.of(t2, t1);

        UserInfoPDV userInfo = new UserInfoPDV();
        userInfo.setPii("FAKE_CF");
        when(userRestClient.retrieveUserInfo("U1"))
                .thenReturn(Mono.just(userInfo));

        when(rewardTransactionRepository.findByFilter(any(), eq("U1"), eq(pageable)))
                .thenReturn(Flux.fromIterable(ordered));

        when(rewardTransactionRepository.getCount(any(), any(), any(), any(), eq("U1"), any()))
                .thenReturn(Mono.just(2L));

        var method = MerchantTransactionServiceImpl.class
                .getDeclaredMethod("getMerchantTransactionDTOs",
                        TrxFiltersDTO.class, String.class, Pageable.class);
        method.setAccessible(true);

        TrxFiltersDTO filters = new TrxFiltersDTO("M", initiativeId, null, "STATUS", null, null, null);

        Mono<Tuple2<List<MerchantTransactionDTO>, Long>> result =
                (Mono<Tuple2<List<MerchantTransactionDTO>, Long>>) method.invoke(service,
                        filters, "U1", pageable);

        StepVerifier.create(result)
                .assertNext(tuple -> {
                    assert tuple.getT2() == 2L;
                    assert tuple.getT1().size() == 2;
                })
                .verifyComplete();
    }

}
