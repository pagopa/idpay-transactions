package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MerchantTransactionServiceImplTest {

    @Mock
    private RewardTransactionRepository rewardTransactionRepositoryMock;
    @Mock
    private UserRestClient userRestClientMock;
    private final ChecksErrorMapper checksErrorMapper = new ChecksErrorMapper();

    private MerchantTransactionService merchantTransactionService;

    private static final String INITIATIVE_ID = "INITIATIVEID1";
    private static final String MERCHANT_ID = "MERCHANTID1";
    private static final String USER_ID = "USERID1";
    private static final String FISCAL_CODE = "FISCALCODE1";

    @BeforeEach
    void setUp() {
        merchantTransactionService =
                new MerchantTransactionServiceImpl(userRestClientMock, rewardTransactionRepositoryMock, checksErrorMapper);
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

        when(userRestClientMock.retrieveFiscalCodeInfo(anyString()))
                .thenReturn(Mono.just(fiscalCodeInfo));

        when(rewardTransactionRepositoryMock.findByFilter(any(), eq(USER_ID), eq(false), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepositoryMock.getCount(any(), isNull(), isNull(), eq(USER_ID), eq(false)))
                .thenReturn(Mono.just(1L));

        MerchantTransactionsListDTO result =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        FISCAL_CODE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                ).block();

        assertFirstPage(result);

        List<MerchantTransactionDTO> content = Objects.requireNonNull(result).getContent();
        assertNotNull(content);
        assertFalse(content.isEmpty());

        MerchantTransactionDTO dto = content.getFirst();
        assertMerchantTransactionMatches(rt1, dto, FISCAL_CODE);

        verify(userRestClientMock).retrieveFiscalCodeInfo(FISCAL_CODE);
        verify(userRestClientMock, never()).retrieveUserInfo(anyString());
        verify(rewardTransactionRepositoryMock).findByFilter(any(), eq(USER_ID), eq(false), eq(paging));
        verify(rewardTransactionRepositoryMock).getCount(any(), isNull(), isNull(), eq(USER_ID), eq(false));
        verifyNoMoreInteractions(rewardTransactionRepositoryMock);
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

        when(rewardTransactionRepositoryMock.findByFilter(any(), isNull(), eq(false), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepositoryMock.getCount(any(), isNull(), isNull(), isNull(), eq(false)))
                .thenReturn(Mono.just(1L));

        when(userRestClientMock.retrieveUserInfo(USER_ID))
                .thenReturn(Mono.just(userInfoPDV));

        MerchantTransactionsListDTO result =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                ).block();

        assertFirstPage(result);

        List<MerchantTransactionDTO> content = Objects.requireNonNull(result).getContent();
        assertNotNull(content);
        assertFalse(content.isEmpty());

        MerchantTransactionDTO dto = content.getFirst();
        assertMerchantTransactionMatches(rt1, dto, FISCAL_CODE);

        verify(rewardTransactionRepositoryMock).findByFilter(any(), isNull(), eq(false), eq(paging));
        verify(rewardTransactionRepositoryMock).getCount(any(), isNull(), isNull(), isNull(), eq(false));
        verify(userRestClientMock).retrieveUserInfo(USER_ID);
        verify(userRestClientMock, never()).retrieveFiscalCodeInfo(anyString());
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
                        null,
                        paging
                ).block()
        );

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatusCode());
        assertEquals(
                "400 BAD_REQUEST \"Invalid rewardBatchTrxStatus value: WRONG_STATUS\"",
                ex.getMessage()
        );

        verifyNoInteractions(rewardTransactionRepositoryMock, userRestClientMock);
    }

    /**
     * Non-operator + transaction TO_CHECK -> DTO espone CONSULTABLE.
     */
    @Test
    void getMerchantTransactionList_nonOperatorToCheckExposedAsConsultable() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(now)
                .rewards(getReward())
                .trxDate(now)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();

        Pageable paging = PageRequest.of(
                0,
                10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending()
        );

        FiscalCodeInfoPDV fiscalCodeInfo = new FiscalCodeInfoPDV(USER_ID);

        when(userRestClientMock.retrieveFiscalCodeInfo(FISCAL_CODE))
                .thenReturn(Mono.just(fiscalCodeInfo));

        when(rewardTransactionRepositoryMock.findByFilter(any(), eq(USER_ID), eq(false), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepositoryMock.getCount(any(), isNull(), isNull(), eq(USER_ID), eq(false)))
                .thenReturn(Mono.just(1L));

        MerchantTransactionsListDTO result =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        FISCAL_CODE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                ).block();

        assertFirstPage(result);

        List<MerchantTransactionDTO> content = Objects.requireNonNull(result).getContent();
        assertNotNull(content);
        assertFalse(content.isEmpty());

        MerchantTransactionDTO dto = content.getFirst();

        assertEquals(RewardBatchTrxStatus.CONSULTABLE, dto.getRewardBatchTrxStatus());
        assertEquals(FISCAL_CODE, dto.getFiscalCode());
    }

    /**
     * Non-operator + filtro rewardBatchTrxStatus=CONSULTABLE:
     * - il filtro rimane CONSULTABLE
     * - includeToCheckWithConsultable viene passato a true verso il repository
     *   (CONSULTABLE + TO_CHECK).
     */
    @Test
    void getMerchantTransactions_nonOperatorConsultableFilterIncludesToCheck() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(now)
                .rewards(getReward())
                .trxDate(now)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();

        Pageable paging = PageRequest.of(
                0,
                10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending()
        );

        UserInfoPDV userInfoPDV = new UserInfoPDV(FISCAL_CODE);

        when(rewardTransactionRepositoryMock.findByFilter(any(), isNull(), eq(true), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepositoryMock.getCount(any(), isNull(), isNull(), isNull(), eq(true)))
                .thenReturn(Mono.just(1L));

        when(userRestClientMock.retrieveUserInfo(USER_ID))
                .thenReturn(Mono.just(userInfoPDV));

        MerchantTransactionsListDTO result =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        RewardBatchTrxStatus.CONSULTABLE.name(),
                        null,
                        null,
                        paging
                ).block();

        assertFirstPage(result);

        ArgumentCaptor<TrxFiltersDTO> filtersCaptor = ArgumentCaptor.forClass(TrxFiltersDTO.class);
        ArgumentCaptor<Boolean> includeCaptor = ArgumentCaptor.forClass(Boolean.class);

        verify(rewardTransactionRepositoryMock).findByFilter(
                filtersCaptor.capture(),
                isNull(),
                includeCaptor.capture(),
                eq(paging)
        );

        TrxFiltersDTO effectiveFilters = filtersCaptor.getValue();
        assertNotNull(effectiveFilters);
        assertEquals(RewardBatchTrxStatus.CONSULTABLE, effectiveFilters.getRewardBatchTrxStatus());

        Boolean includeFlag = includeCaptor.getValue();
        assertNotNull(includeFlag);
        assertTrue(includeFlag, "Per i non-operator con filtro CONSULTABLE il flag includeToCheckWithConsultable deve essere true");
    }

    @Test
    void getMerchantTransactionList_withChecksError() {
        LocalDateTime now = LocalDateTime.now();

        ChecksError checksError = new ChecksError(true,true,true,true,true,true,true);

        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .elaborationDateTime(now)
                .rewards(getReward())
                .trxDate(now)
                .checksError(checksError)
                .build();

        Pageable paging = PageRequest.of(
                0,
                10,
                Sort.by(RewardTransaction.Fields.elaborationDateTime).descending()
        );

        UserInfoPDV userInfoPDV = new UserInfoPDV(FISCAL_CODE);

        when(rewardTransactionRepositoryMock.findByFilter(any(), isNull(), eq(false), eq(paging)))
                .thenReturn(Flux.just(rt1));

        when(rewardTransactionRepositoryMock.getCount(any(), isNull(), isNull(), isNull(), eq(false)))
                .thenReturn(Mono.just(1L));

        when(userRestClientMock.retrieveUserInfo(USER_ID))
                .thenReturn(Mono.just(userInfoPDV));

        MerchantTransactionsListDTO result =
                merchantTransactionService.getMerchantTransactions(
                        MERCHANT_ID,
                        "merchant",
                        INITIATIVE_ID,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        paging
                ).block();

        assertFirstPage(result);

        List<MerchantTransactionDTO> content = Objects.requireNonNull(result).getContent();
        assertNotNull(content);
        assertFalse(content.isEmpty());

        MerchantTransactionDTO dto = content.getFirst();
        assertMerchantTransactionMatches(rt1, dto, FISCAL_CODE);
        assertNotNull(dto.getChecksError());

        verify(rewardTransactionRepositoryMock).findByFilter(any(), isNull(), eq(false), eq(paging));
        verify(rewardTransactionRepositoryMock).getCount(any(), isNull(), isNull(), isNull(), eq(false));
        verify(userRestClientMock).retrieveUserInfo(USER_ID);
        verify(userRestClientMock, never()).retrieveFiscalCodeInfo(anyString());
    }

    @Test
    void getProcessedTransactionStatuses_operatorVsNonOperator() {
        List<String> operatorStatuses = merchantTransactionService
                .getProcessedTransactionStatuses(
                        "operator1").block();

        List<String> merchantStatuses = merchantTransactionService
                .getProcessedTransactionStatuses(
                        "merchant").block();

        assertNotNull(operatorStatuses);
        assertNotNull(merchantStatuses);

        List<String> allEnumStatuses = Arrays.stream(RewardBatchTrxStatus.values())
                .map(Enum::name)
                .toList();

        assertEquals(allEnumStatuses.size(), operatorStatuses.size());
        assertTrue(operatorStatuses.contains(RewardBatchTrxStatus.TO_CHECK.name()));

        assertFalse(merchantStatuses.contains(RewardBatchTrxStatus.TO_CHECK.name()));
        assertEquals(allEnumStatuses.size() - 1, merchantStatuses.size());
    }

    private void assertFirstPage(MerchantTransactionsListDTO result) {
        assertNotNull(result);
        assertEquals(0, result.getPageNo());
        assertEquals(10, result.getPageSize());
        assertEquals(1, result.getTotalElements());
        assertEquals(1, result.getTotalPages());

        List<MerchantTransactionDTO> content = result.getContent();
        assertNotNull(content);
        assertEquals(1, content.size());
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
