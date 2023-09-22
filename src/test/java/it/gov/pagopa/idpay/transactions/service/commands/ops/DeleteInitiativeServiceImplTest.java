package it.gov.pagopa.idpay.transactions.service.commands.ops;

import com.mongodb.MongoException;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.dto.QueueCommandOperationDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class DeleteInitiativeServiceImplTest {
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private AuditUtilities auditUtilitiesMock;

    private DeleteInitiativeService deleteInitiativeService;

    private static final String INITIATIVE_ID = "INITIATIVEID";
    private static final String OPERATION_TYPE_DELETE_INITIATIVE = "DELETE_INITIATIVE";
    private static final String PAGINATION_KEY = "pagination";
    private static final String PAGINATION_VALUE = "100";
    private static final String DELAY_KEY = "delay";
    private static final String DELAY_VALUE = "1500";
    private static final Map<String, String> ADDITIONAL_PARAMS = new HashMap<>() {{ put(PAGINATION_KEY, PAGINATION_VALUE); put(DELAY_KEY, DELAY_VALUE); }};
    private static final QueueCommandOperationDTO QUEUE_COMMAND_OPERATION_DTO = QueueCommandOperationDTO.builder()
            .entityId(INITIATIVE_ID)
            .operationTime(LocalDateTime.now().minusMinutes(5))
            .operationType(OPERATION_TYPE_DELETE_INITIATIVE)
            .additionalParams(ADDITIONAL_PARAMS)
            .build();

    @BeforeEach
    void setUp() {
        deleteInitiativeService = new DeleteInitiativeServiceImpl(
                rewardTransactionRepository,
                auditUtilitiesMock);
    }

    @Test
    void executeOK_delete() {
        // Given
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(INITIATIVE_ID));
        trx.setChannel("QRCODE");

        DeleteResult deleteResult1 = DeleteResult.acknowledged(100);
        DeleteResult deleteResult2 = DeleteResult.acknowledged(63);

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(INITIATIVE_ID))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.deleteByInitiativeIdPaged(INITIATIVE_ID, Integer.parseInt(PAGINATION_VALUE)))
                .thenReturn(Mono.just(deleteResult1))
                .thenReturn(Mono.just(deleteResult2));

        // When
        String result = deleteInitiativeService.execute(QUEUE_COMMAND_OPERATION_DTO).block();

        // Then
        Assertions.assertNotNull(result);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findOneByInitiativeId(Mockito.anyString());
        Mockito.verify(rewardTransactionRepository, Mockito.times(2)).deleteByInitiativeIdPaged(INITIATIVE_ID, 100);
        Mockito.verify(rewardTransactionRepository, Mockito.times(0)).findAndRemoveInitiativeOnTransactionPaged(INITIATIVE_ID, 100);
    }

    @Test
    void executeOK_findAndRemove() {
        // Given
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(INITIATIVE_ID));
        trx.setChannel("RTD");

        UpdateResult updateResult1 = UpdateResult.acknowledged(163,100L, null);
        UpdateResult updateResult2 = UpdateResult.acknowledged(163,63L, null);

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(INITIATIVE_ID))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.findAndRemoveInitiativeOnTransactionPaged(INITIATIVE_ID, Integer.parseInt(PAGINATION_VALUE)))
                .thenReturn(Mono.just(updateResult1))
                .thenReturn(Mono.just(updateResult2));

        // When
        String result = deleteInitiativeService.execute(QUEUE_COMMAND_OPERATION_DTO).block();

        // Then
        Assertions.assertNotNull(result);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findOneByInitiativeId(Mockito.anyString());
        Mockito.verify(rewardTransactionRepository, Mockito.times(0)).deleteByInitiativeIdPaged(INITIATIVE_ID, 100);
        Mockito.verify(rewardTransactionRepository, Mockito.times(2)).findAndRemoveInitiativeOnTransactionPaged(INITIATIVE_ID, 100);
    }

    @Test
    void executeError() {
        String initiativeId = "INITIATIVEID";
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(initiativeId));
        trx.setChannel("QRCODE");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(initiativeId))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.deleteByInitiativeIdPaged(INITIATIVE_ID, Integer.parseInt(PAGINATION_VALUE)))
                .thenThrow(new MongoException("DUMMY_EXCEPTION"));

        try{
            deleteInitiativeService.execute(QUEUE_COMMAND_OPERATION_DTO).block();
            Assertions.fail();
        }catch (Throwable t){
            Assertions.assertTrue(t instanceof  MongoException);
        }
    }
}