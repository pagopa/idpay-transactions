package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionMutationPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoSuspendedTransactionReassignmentAdapterTest {

    private static final String SOURCE_BATCH_ID = "source";
    private static final String INITIATIVE_ID = "initiative";
    private static final String MERCHANT_ID = "merchant";
    private static final String BUSINESS_NAME = "business";

    @Mock
    private RewardBatchRepository rewardBatchRepository;
    @Mock
    private RewardBatchTransactionMutationPort rewardBatchTransactionMutationPort;

    @Test
    void reassignSuspendedTransactions_createsCurrentMonthTargetForPastSourceAndDelegatesMove() {
        String sourceMonth = YearMonth.now(ZONEID).minusMonths(1).toString();
        RewardBatch source = source(sourceMonth, 1L);
        String targetMonth = YearMonth.now(ZONEID).toString();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(SOURCE_BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(source));
        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID, MERCHANT_ID, PosType.PHYSICAL, targetMonth
        )).thenReturn(Mono.empty());
        when(rewardBatchRepository.save(any(RewardBatch.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(rewardBatchTransactionMutationPort.reassignSuspendedTransactions(
                same(source), any(RewardBatch.class), eq(INITIATIVE_ID), eq(sourceMonth)
        )).thenReturn(Mono.empty());

        StepVerifier.create(adapter().reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                .verifyComplete();

        ArgumentCaptor<RewardBatch> targetCaptor = ArgumentCaptor.forClass(RewardBatch.class);
        verify(rewardBatchRepository).save(targetCaptor.capture());
        RewardBatch target = targetCaptor.getValue();
        assertEquals(targetMonth, target.getMonth());
        assertEquals(INITIATIVE_ID, target.getInitiativeId());
        assertEquals(MERCHANT_ID, target.getMerchantId());
        verify(rewardBatchTransactionMutationPort).reassignSuspendedTransactions(
                same(source), same(target), eq(INITIATIVE_ID), eq(sourceMonth)
        );
    }

    @Test
    void reassignSuspendedTransactions_reusesFutureMonthTarget() {
        String futureMonth = YearMonth.now(ZONEID).plusMonths(1).toString();
        RewardBatch source = source(futureMonth, 1L);
        RewardBatch target = RewardBatch.builder().id("target").month(futureMonth).build();
        AtomicBoolean targetCreationSubscribed = new AtomicBoolean();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(SOURCE_BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(source));
        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID, MERCHANT_ID, PosType.PHYSICAL, futureMonth
        )).thenReturn(Mono.just(target));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenReturn(Mono.defer(() -> {
            targetCreationSubscribed.set(true);
            return Mono.just(target);
        }));
        when(rewardBatchTransactionMutationPort.reassignSuspendedTransactions(
                same(source), same(target), eq(INITIATIVE_ID), eq(futureMonth)
        )).thenReturn(Mono.empty());

        StepVerifier.create(adapter().reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                .verifyComplete();

        assertFalse(targetCreationSubscribed.get());
        verify(rewardBatchTransactionMutationPort).reassignSuspendedTransactions(
                same(source), same(target), eq(INITIATIVE_ID), eq(futureMonth)
        );
    }

    @Test
    void reassignSuspendedTransactions_retriesTargetLookupAfterDuplicateCreation() {
        String sourceMonth = YearMonth.now(ZONEID).toString();
        RewardBatch source = source(sourceMonth, 1L);
        RewardBatch target = RewardBatch.builder().id("concurrently-created").month(sourceMonth).build();

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(SOURCE_BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(source));
        when(rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                INITIATIVE_ID, MERCHANT_ID, PosType.PHYSICAL, sourceMonth
        )).thenReturn(Mono.empty(), Mono.just(target));
        when(rewardBatchRepository.save(any(RewardBatch.class)))
                .thenReturn(Mono.error(new DuplicateKeyException("duplicate batch")));
        when(rewardBatchTransactionMutationPort.reassignSuspendedTransactions(
                same(source), same(target), eq(INITIATIVE_ID), eq(sourceMonth)
        )).thenReturn(Mono.empty());

        StepVerifier.create(adapter().reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                .verifyComplete();

        verify(rewardBatchRepository, times(2))
                .findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                        INITIATIVE_ID, MERCHANT_ID, PosType.PHYSICAL, sourceMonth
                );
        verify(rewardBatchTransactionMutationPort).reassignSuspendedTransactions(
                same(source), same(target), eq(INITIATIVE_ID), eq(sourceMonth)
        );
    }

    @Test
    void reassignSuspendedTransactions_doesNothingWhenSourceIsMissingOrHasNoSuspendedTransactions() {
        RewardBatch emptySource = source(YearMonth.now(ZONEID).toString(), 0L);
        RewardBatch sourceWithoutSuspendedCount = source(YearMonth.now(ZONEID).toString(), null);

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(SOURCE_BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.empty(), Mono.just(emptySource), Mono.just(sourceWithoutSuspendedCount));

        StepVerifier.create(adapter().reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                .verifyComplete();
        StepVerifier.create(adapter().reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                .verifyComplete();
        StepVerifier.create(adapter().reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                .verifyComplete();

        verify(rewardBatchRepository, never()).findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                any(), any(), any(), any()
        );
        verify(rewardBatchTransactionMutationPort, never()).reassignSuspendedTransactions(
                any(), any(), any(), any()
        );
    }

    private MongoSuspendedTransactionReassignmentAdapter adapter() {
        return new MongoSuspendedTransactionReassignmentAdapter(
                rewardBatchRepository,
                rewardBatchTransactionMutationPort
        );
    }

    private static RewardBatch source(String month, Long suspendedTransactions) {
        return RewardBatch.builder()
                .id(SOURCE_BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .merchantId(MERCHANT_ID)
                .businessName(BUSINESS_NAME)
                .posType(PosType.PHYSICAL)
                .month(month)
                .numberOfTransactionsSuspended(suspendedTransactions)
                .build();
    }
}
