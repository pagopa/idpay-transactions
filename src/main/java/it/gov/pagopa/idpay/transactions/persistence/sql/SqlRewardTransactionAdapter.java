package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.excluded;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSynchronizationPort;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardTransactionsRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SqlRewardTransactionAdapter implements RewardTransactionSynchronizationPort {

    private final TransactionalOperator transactionalOperator;
    private final DSLContext dslContext;
    private final RewardTransactionSqlRepository repository;
    private final RewardTransactionSqlMapper mapper;

    @Override
    public Mono<RewardTransaction> upsert(RewardTransaction transaction) {
        RewardTransactionEntity entity = mapper.toEntity(transaction);
        Mono<RewardTransaction> persisted = Mono.from(insertOrUpdate(mapper.toRecord(entity)))
                .flatMap(ignored -> repository.findById(entity.id()))
                .map(mapper::fromEntity);
        Mono<RewardTransaction> rejected = repository.findById(entity.id())
                        .flatMap(existing -> Mono.<RewardTransaction>error(new IllegalStateException(
                                "Transaction %s already belongs to initiative %s"
                                        .formatted(entity.id(), existing.initiativeId())
                        )))
                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                "Transaction %s was not persisted".formatted(entity.id())
                        )));
        return transactionalOperator.transactional(persisted.switchIfEmpty(rejected));
    }

    private org.jooq.InsertResultStep<?> insertOrUpdate(RewardTransactionsRecord transactionRecord) {
        return dslContext.insertInto(REWARD_TRANSACTIONS)
                .set(transactionRecord)
                .onConflict(REWARD_TRANSACTIONS.TRANSACTION_ID)
                .doUpdate()
                .set(REWARD_TRANSACTIONS.ID_TRX_ACQUIRER, excluded(REWARD_TRANSACTIONS.ID_TRX_ACQUIRER))
                .set(REWARD_TRANSACTIONS.ACQUIRER_CODE, excluded(REWARD_TRANSACTIONS.ACQUIRER_CODE))
                .set(REWARD_TRANSACTIONS.TRX_DATE, excluded(REWARD_TRANSACTIONS.TRX_DATE))
                .set(REWARD_TRANSACTIONS.OPERATION_TYPE, excluded(REWARD_TRANSACTIONS.OPERATION_TYPE))
                .set(REWARD_TRANSACTIONS.CIRCUIT_TYPE, excluded(REWARD_TRANSACTIONS.CIRCUIT_TYPE))
                .set(REWARD_TRANSACTIONS.ID_TRX_ISSUER, excluded(REWARD_TRANSACTIONS.ID_TRX_ISSUER))
                .set(REWARD_TRANSACTIONS.CORRELATION_ID, excluded(REWARD_TRANSACTIONS.CORRELATION_ID))
                .set(REWARD_TRANSACTIONS.AMOUNT_CENTS, excluded(REWARD_TRANSACTIONS.AMOUNT_CENTS))
                .set(REWARD_TRANSACTIONS.AMOUNT_CURRENCY, excluded(REWARD_TRANSACTIONS.AMOUNT_CURRENCY))
                .set(REWARD_TRANSACTIONS.ACQUIRER_ID, excluded(REWARD_TRANSACTIONS.ACQUIRER_ID))
                .set(REWARD_TRANSACTIONS.MERCHANT_ID, excluded(REWARD_TRANSACTIONS.MERCHANT_ID))
                .set(REWARD_TRANSACTIONS.POINT_OF_SALE_ID, excluded(REWARD_TRANSACTIONS.POINT_OF_SALE_ID))
                .set(REWARD_TRANSACTIONS.POS_TYPE, excluded(REWARD_TRANSACTIONS.POS_TYPE))
                .set(REWARD_TRANSACTIONS.STATUS, excluded(REWARD_TRANSACTIONS.STATUS))
                .set(REWARD_TRANSACTIONS.REJECTION_REASONS, excluded(REWARD_TRANSACTIONS.REJECTION_REASONS))
                .set(REWARD_TRANSACTIONS.INITIATIVE_REJECTION_REASONS,
                        excluded(REWARD_TRANSACTIONS.INITIATIVE_REJECTION_REASONS))
                .set(REWARD_TRANSACTIONS.REWARDS, excluded(REWARD_TRANSACTIONS.REWARDS))
                .set(REWARD_TRANSACTIONS.USER_ID, excluded(REWARD_TRANSACTIONS.USER_ID))
                .set(REWARD_TRANSACTIONS.OPERATION_TYPE_TRANSCODED,
                        excluded(REWARD_TRANSACTIONS.OPERATION_TYPE_TRANSCODED))
                .set(REWARD_TRANSACTIONS.EFFECTIVE_AMOUNT_CENTS,
                        excluded(REWARD_TRANSACTIONS.EFFECTIVE_AMOUNT_CENTS))
                .set(REWARD_TRANSACTIONS.TRX_CHARGE_DATE, excluded(REWARD_TRANSACTIONS.TRX_CHARGE_DATE))
                .set(REWARD_TRANSACTIONS.REFUND_INFO, excluded(REWARD_TRANSACTIONS.REFUND_INFO))
                .set(REWARD_TRANSACTIONS.ELABORATION_DATE_TIME,
                        excluded(REWARD_TRANSACTIONS.ELABORATION_DATE_TIME))
                .set(REWARD_TRANSACTIONS.CHANNEL, excluded(REWARD_TRANSACTIONS.CHANNEL))
                .set(REWARD_TRANSACTIONS.ADDITIONAL_PROPERTIES,
                        excluded(REWARD_TRANSACTIONS.ADDITIONAL_PROPERTIES))
                .set(REWARD_TRANSACTIONS.INVOICE_DATA, excluded(REWARD_TRANSACTIONS.INVOICE_DATA))
                .set(REWARD_TRANSACTIONS.CREDIT_NOTE_DATA, excluded(REWARD_TRANSACTIONS.CREDIT_NOTE_DATA))
                .set(REWARD_TRANSACTIONS.TRX_CODE, excluded(REWARD_TRANSACTIONS.TRX_CODE))
                .set(REWARD_TRANSACTIONS.UPDATE_DATE, excluded(REWARD_TRANSACTIONS.UPDATE_DATE))
                .set(REWARD_TRANSACTIONS.EXTENDED_AUTHORIZATION,
                        excluded(REWARD_TRANSACTIONS.EXTENDED_AUTHORIZATION))
                .set(REWARD_TRANSACTIONS.VOUCHER_AMOUNT_CENTS,
                        excluded(REWARD_TRANSACTIONS.VOUCHER_AMOUNT_CENTS))
                .set(REWARD_TRANSACTIONS.CHECKS_ERROR, excluded(REWARD_TRANSACTIONS.CHECKS_ERROR))
                .set(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS,
                        excluded(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS))
                .where(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(transactionRecord.getInitiativeId()))
                .returning(REWARD_TRANSACTIONS.TRANSACTION_ID);
    }
}
