package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.LocalDateTime;
import java.util.*;

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
    void testGetMerchantTransactions_withFiscalCode() {
        String merchantId = "M1";
        String initiativeId = "I1";
        String fiscalCode = "AAAAAA11A11A111A";

        Pageable pageable = PageRequest.of(0, 10);

        RewardTransaction t1 = buildRewardTransaction("T1", "USER1", initiativeId);

        when(userRestClient.retrieveFiscalCodeInfo(fiscalCode))
                .thenReturn(Mono.just(new FiscalCodeInfoPDV("TOKEN_USER1")));

        when(rewardTransactionRepository.findByFilter(any(), eq("TOKEN_USER1"), eq(pageable)))
                .thenReturn(Flux.just(t1));

        when(rewardTransactionRepository.getCount(eq(merchantId), eq(initiativeId),
                isNull(), isNull(), eq("TOKEN_USER1"), eq("STATUS")))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getMerchantTransactions(
                        merchantId,
                        initiativeId,
                        fiscalCode,
                        "STATUS",
                        "RB1",
                        RewardBatchStatus.APPROVED.name(),
                        null,
                        pageable
                ))
                .assertNext(result -> {
                    assert result.getTotalElements() == 1;
                })
                .verifyComplete();
    }

    @Test
    void testGetMerchantTransactions_withoutFiscalCode() {
        String merchantId = "M1";
        String initiativeId = "I1";

        Pageable pageable = PageRequest.of(0, 10);

        RewardTransaction t1 = buildRewardTransaction("T1", "USERABC", initiativeId);

        UserInfoPDV user = new UserInfoPDV();
        user.setPii("CF_FROM_PDV");
        when(userRestClient.retrieveUserInfo("USERABC"))
                .thenReturn(Mono.just(user));

        when(rewardTransactionRepository.findByFilter(any(), isNull(), eq(pageable)))
                .thenReturn(Flux.just(t1));

        when(rewardTransactionRepository.getCount(eq(merchantId), eq(initiativeId),
                isNull(), isNull(), isNull(), eq("STATUS")))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(service.getMerchantTransactions(
                        merchantId,
                        initiativeId,
                        null,
                        "STATUS",
                        "RB1",
                        RewardBatchStatus.APPROVED.name(),
                        null,
                        pageable
                ))
                .assertNext(result -> {
                    assert result.getTotalElements() == 1;
                })
                .verifyComplete();
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
