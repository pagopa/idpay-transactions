package it.gov.pagopa.idpay.transactions.dto.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionMapperTest {

  @Mock
  private UserRestClient userRestClient;

  private PointOfSaleTransactionMapper mapper;

  private static final String INITIATIVE_ID = "INITIATIVEID1";
  private static final String USER_ID = "USERID1";
  private static final String FISCAL_CODE = "FISCALCODE1";


  @BeforeEach
  void setUp() {
    mapper = new PointOfSaleTransactionMapper(userRestClient);
  }

  private static Map<String, Reward> getReward() {
    Map<String, Reward> reward = new HashMap<>();
    RewardCounters counter = RewardCounters.builder()
        .initiativeBudgetCents(10000L)
        .exhaustedBudget(false)
        .build();
    Reward rewardElement = Reward.builder()
        .initiativeId(INITIATIVE_ID)
        .organizationId("ORG")
        .providedRewardCents(1000L)
        .accruedRewardCents(1000L)
        .counters(counter)
        .build();
    reward.put(INITIATIVE_ID, rewardElement);
    return reward;
  }

  @Test
  void toDTO_withFiscalCode_shouldNotCallUserRestClient() {
    RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
        .id("trx1")
        .userId(USER_ID)
        .amountCents(5000L)
        .status("REWARDED")
        .elaborationDateTime(LocalDateTime.now())
        .rewards(getReward())
        .build();

    PointOfSaleTransactionDTO result = mapper.toDTO(trx, INITIATIVE_ID, FISCAL_CODE).block();

    assertNotNull(result);
    assertEquals(trx.getId(), result.getTrxId());
    assertEquals(5000L, result.getEffectiveAmountCents());
    assertEquals(1000L, result.getRewardAmountCents());
    assertEquals("REWARDED", result.getStatus());
    assertEquals(FISCAL_CODE, result.getFiscalCode());

    verifyNoInteractions(userRestClient);
  }

  @Test
  void toDTO_withoutFiscalCode_shouldRetrieveFromUserRestClient() {
    RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
        .id("trx2")
        .userId(USER_ID)
        .amountCents(5000L)
        .status("REWARDED")
        .elaborationDateTime(LocalDateTime.now())
        .rewards(getReward())
        .build();

    Mockito.when(userRestClient.retrieveUserInfo(anyString()))
        .thenReturn(Mono.just(new UserInfoPDV(FISCAL_CODE)));

    PointOfSaleTransactionDTO result = mapper.toDTO(trx, INITIATIVE_ID, null).block();

    assertNotNull(result);
    assertEquals(trx.getId(), result.getTrxId());
    assertEquals(5000L, result.getEffectiveAmountCents());
    assertEquals(1000L, result.getRewardAmountCents());
    assertEquals("REWARDED", result.getStatus());
    assertEquals(FISCAL_CODE, result.getFiscalCode());

    verify(userRestClient, times(1)).retrieveUserInfo(USER_ID);
  }
}
