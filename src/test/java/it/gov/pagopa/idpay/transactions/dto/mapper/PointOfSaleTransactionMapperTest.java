package it.gov.pagopa.idpay.transactions.dto.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;
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

    private static final RewardBatchTrxStatus REWARD_BATCH_TRX_STATUS = RewardBatchTrxStatus.values()[0];

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

    private static RewardTransaction baseTrx(String id, long amountCents, String status) {
        return RewardTransaction.builder()
                .id(id)
                .userId(USER_ID)
                .amountCents(amountCents)
                .status(status)
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
                .elaborationDateTime(LocalDateTime.now())
                .build();
    }

    @Test
    void toDTO_withFiscalCode_shouldNotCallUserRestClient() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("trx1")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
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
        assertEquals(trx.getRewardBatchTrxStatus().toString(), result.getRewardBatchTrxStatus());

        verifyNoInteractions(userRestClient);
    }

    @Test
    void toDTO_withoutFiscalCode_shouldRetrieveFromUserRestClient() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("trx2")
                .userId(USER_ID)
                .amountCents(5000L)
                .status("REWARDED")
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
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
        assertEquals(trx.getRewardBatchTrxStatus().toString(), result.getRewardBatchTrxStatus());

        verify(userRestClient, times(1)).retrieveUserInfo(USER_ID);
    }

    @Test
    void toDTO_shouldHandleNullRewards() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("trx3")
                .userId(USER_ID)
                .amountCents(8000L)
                .status("CANCELLED")
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
                .elaborationDateTime(LocalDateTime.now())
                .rewards(null)
                .build();

        PointOfSaleTransactionDTO result = mapper.toDTO(trx, INITIATIVE_ID, FISCAL_CODE).block();

        assertNotNull(result);
        assertEquals(trx.getId(), result.getTrxId());
        assertEquals(8000L, result.getEffectiveAmountCents());
        assertEquals(0L, result.getRewardAmountCents());
        assertEquals(8000L, result.getAuthorizedAmountCents());
        assertEquals("CANCELLED", result.getStatus());
        assertEquals(FISCAL_CODE, result.getFiscalCode());
        assertEquals(trx.getRewardBatchTrxStatus().toString(), result.getRewardBatchTrxStatus());

        verifyNoInteractions(userRestClient);
    }

    @Test
    void toDTO_shouldHandleMissingInitiativeReward() {
        Map<String, Reward> rewards = new HashMap<>();
        Reward reward = Reward.builder()
                .initiativeId("OTHER_INITIATIVE")
                .accruedRewardCents(2000L)
                .build();
        rewards.put("OTHER_INITIATIVE", reward);

        RewardTransaction trx = RewardTransaction.builder()
                .id("trx4")
                .userId(USER_ID)
                .amountCents(10000L)
                .status("CANCELLED")
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
                .elaborationDateTime(LocalDateTime.now())
                .rewards(rewards)
                .build();

        PointOfSaleTransactionDTO result = mapper.toDTO(trx, INITIATIVE_ID, FISCAL_CODE).block();

        assertNotNull(result);
        assertEquals(trx.getId(), result.getTrxId());
        assertEquals(10000L, result.getEffectiveAmountCents());
        assertEquals(0L, result.getRewardAmountCents());
        assertEquals(10000L, result.getAuthorizedAmountCents());
        assertEquals("CANCELLED", result.getStatus());
        assertEquals(FISCAL_CODE, result.getFiscalCode());
        assertEquals(trx.getRewardBatchTrxStatus().toString(), result.getRewardBatchTrxStatus());

        verifyNoInteractions(userRestClient);
    }

    @Test
    void toDTO_shouldSetInvoiceFile_whenInvoiced() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("trx5")
                .userId(USER_ID)
                .amountCents(7000L)
                .status("INVOICED")
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
                .invoiceData(InvoiceData.builder()
                        .filename("invoice.pdf")
                        .docNumber("DOC123")
                        .build())
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward())
                .build();

        PointOfSaleTransactionDTO result = mapper.toDTO(trx, INITIATIVE_ID, FISCAL_CODE).block();

        assertNotNull(result);
        assertNotNull(result.getInvoiceFile());
        assertEquals("invoice.pdf", result.getInvoiceFile().getFilename());
        assertEquals("DOC123", result.getInvoiceFile().getDocNumber());
        assertEquals(trx.getRewardBatchTrxStatus().toString(), result.getRewardBatchTrxStatus());
    }

    @Test
    void toDTO_shouldSetInvoiceFile_whenRefunded() {
        RewardTransaction trx = RewardTransaction.builder()
                .id("trx6")
                .userId(USER_ID)
                .amountCents(9000L)
                .status("REFUNDED")
                .rewardBatchTrxStatus(REWARD_BATCH_TRX_STATUS)
                .creditNoteData(InvoiceData.builder()
                        .filename("creditNote.pdf")
                        .docNumber("CN456")
                        .build())
                .elaborationDateTime(LocalDateTime.now())
                .rewards(getReward())
                .build();

        PointOfSaleTransactionDTO result = mapper.toDTO(trx, INITIATIVE_ID, FISCAL_CODE).block();

        assertNotNull(result);
        assertEquals(trx.getId(), result.getTrxId());
        assertEquals(9000L, result.getEffectiveAmountCents());
        assertEquals(1000L, result.getRewardAmountCents());
        assertEquals(8000L, result.getAuthorizedAmountCents());
        assertEquals("REFUNDED", result.getStatus());
        assertEquals(FISCAL_CODE, result.getFiscalCode());
        assertEquals(trx.getRewardBatchTrxStatus().toString(), result.getRewardBatchTrxStatus());

        assertNotNull(result.getInvoiceFile());
        assertEquals("creditNote.pdf", result.getInvoiceFile().getFilename());
        assertEquals("CN456", result.getInvoiceFile().getDocNumber());

        verifyNoInteractions(userRestClient);
    }
}
