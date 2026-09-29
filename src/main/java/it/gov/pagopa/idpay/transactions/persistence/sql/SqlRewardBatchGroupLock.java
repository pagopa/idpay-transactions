package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.jooq.impl.DSL.val;

import java.util.Objects;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import reactor.core.publisher.Mono;

/**
 * Acquires the grouping advisory lock that precedes all reward-batch row
 * locks. Callers must not acquire a batch row before this lock.
 */
final class SqlRewardBatchGroupLock {

    private SqlRewardBatchGroupLock() {
    }

    static Mono<Void> acquire(
            DSLContext transactionDslContext,
            String initiativeId,
            String merchantId,
            String posType
    ) {
        long lockKey = Objects.hash("reward-batch", initiativeId, merchantId, posType);
        return Mono.from(transactionDslContext.select(
                        DSL.field("pg_advisory_xact_lock({0})", Object.class, val(lockKey))
                ))
                .then();
    }
}
