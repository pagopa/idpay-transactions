package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionsControllerImplTest {

    @Mock
    private RewardTransactionService rewardTransactionService;

    @InjectMocks
    private TransactionsControllerImpl controller;

    @Test
    void findAll_withIdTrxIssuer_shouldCallFindByIdTrxIssuer() {
        LocalDateTime now = LocalDateTime.of(2022, 9, 20, 13, 15, 45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);
        String idTrxIssuer = "IDTRXISSUER";
        String userId = "USERID";
        Long amountCents = 3000L;

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer(idTrxIssuer)
                .userId(userId)
                .trxDate(now)
                .amountCents(amountCents)
                .build();

        Pageable pageable = PageRequest.of(0, 10, Sort.by("trxDate").descending());

        when(rewardTransactionService.findByIdTrxIssuer(
                idTrxIssuer,
                userId,
                startDate,
                endDate,
                amountCents,
                pageable
        )).thenReturn(Flux.just(rt));

        List<RewardTransaction> result = controller.findAll(
                idTrxIssuer,
                userId,
                startDate,
                endDate,
                amountCents,
                pageable
        ).collectList().block();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(rt, result.getFirst());

        verify(rewardTransactionService, times(1))
                .findByIdTrxIssuer(idTrxIssuer, userId, startDate, endDate, amountCents, pageable);
        verify(rewardTransactionService, never())
                .findByRange(any(), any(), any(), any(), any());
    }

    @Test
    void findAll_withUserIdAndRange_shouldCallFindByRange() {
        LocalDateTime now = LocalDateTime.of(2022, 9, 20, 13, 15, 45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);
        String userId = "USERID";
        Long amountCents = 3000L;

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(now)
                .amountCents(amountCents)
                .build();

        Pageable pageable = PageRequest.of(1, 5, Sort.unsorted());

        when(rewardTransactionService.findByRange(
                userId,
                startDate,
                endDate,
                amountCents,
                pageable
        )).thenReturn(Flux.just(rt));

        List<RewardTransaction> result = controller.findAll(
                null,
                userId,
                startDate,
                endDate,
                amountCents,
                pageable
        ).collectList().block();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(rt, result.getFirst());

        verify(rewardTransactionService, times(1))
                .findByRange(userId, startDate, endDate, amountCents, pageable);
        verify(rewardTransactionService, never())
                .findByIdTrxIssuer(any(), any(), any(), any(), any(), any());
    }

    @Test
    void findAll_missingMandatoryFilters_shouldThrowClientException() {
        Long amountCents = 3000L;
        Pageable pageable = PageRequest.of(0, 10);

        ClientExceptionWithBody ex1 = assertThrows(
                ClientExceptionWithBody.class,
                () -> controller.findAll(
                        null,
                        null,
                        null,
                        null,
                        amountCents,
                        pageable
                )
        );
        assertEquals(ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ex1.getCode());
        assertEquals(ExceptionConstants.ExceptionMessage.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ex1.getMessage());

        LocalDateTime now = LocalDateTime.now();
        ClientExceptionWithBody ex2 = assertThrows(
                ClientExceptionWithBody.class,
                () -> controller.findAll(
                        null,
                        "USERID",
                        now,
                        null,
                        amountCents,
                        pageable
                )
        );
        assertEquals(ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ex2.getCode());

        verify(rewardTransactionService, never()).findByIdTrxIssuer(any(), any(), any(), any(), any(), any());
        verify(rewardTransactionService, never()).findByRange(any(), any(), any(), any(), any());
    }

    @Test
    void findAll_pageableShouldBePassedToService() {
        LocalDateTime now = LocalDateTime.of(2022, 9, 20, 13, 15, 45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);
        String userId = "USERID";
        String idTrxIssuer = "IDTRXISSUER";
        Long amountCents = 3000L;

        RewardTransaction rt = RewardTransaction.builder()
                .id("ID1")
                .idTrxIssuer(idTrxIssuer)
                .userId(userId)
                .trxDate(now)
                .amountCents(amountCents)
                .build();

        Pageable pageable = PageRequest.of(2, 3, Sort.unsorted());

        when(rewardTransactionService.findByIdTrxIssuer(
                idTrxIssuer,
                userId,
                startDate,
                endDate,
                amountCents,
                pageable
        )).thenReturn(Flux.just(rt));

        List<RewardTransaction> result = controller.findAll(
                idTrxIssuer,
                userId,
                startDate,
                endDate,
                amountCents,
                pageable
        ).collectList().block();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(rt, result.getFirst());

        verify(rewardTransactionService, times(1))
                .findByIdTrxIssuer(idTrxIssuer, userId, startDate, endDate, amountCents, pageable);
    }

    @Test
    void findByTrxIdAndUserId_ok_shouldCallService() {
        RewardTransaction rt = RewardTransaction.builder()
                .id("TRXID")
                .userId("USERID")
                .build();

        when(rewardTransactionService.findByTrxIdAndUserId("TRXID", "USERID"))
                .thenReturn(Mono.just(rt));

        RewardTransaction result = controller.findByTrxIdAndUserId("TRXID", "USERID").block();

        assertNotNull(result);
        assertEquals("TRXID", result.getId());
        assertEquals("USERID", result.getUserId());

        verify(rewardTransactionService, times(1))
                .findByTrxIdAndUserId("TRXID", "USERID");
    }

    @Test
    void findByTrxIdAndUserId_missingParams_shouldThrowClientException() {
        ClientExceptionWithBody ex1 = assertThrows(
                ClientExceptionWithBody.class,
                () -> controller.findByTrxIdAndUserId(null, "USERID")
        );
        assertEquals(ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ex1.getCode());

        ClientExceptionWithBody ex2 = assertThrows(
                ClientExceptionWithBody.class,
                () -> controller.findByTrxIdAndUserId("TRXID", null)
        );
        assertEquals(ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ex2.getCode());

        verify(rewardTransactionService, never()).findByTrxIdAndUserId(any(), any());
    }
}
