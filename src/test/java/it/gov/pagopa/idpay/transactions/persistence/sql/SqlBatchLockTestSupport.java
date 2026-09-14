package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Test-only PostgreSQL lock coordination.
 *
 * <p>The lock holders deliberately use connections that are independent from
 * the connection pool used by the adapter under test. This lets the tests
 * distinguish waiting for the batch row from waiting for the transaction row.
 */
final class SqlBatchLockTestSupport {

    private static final Duration POLL_DELAY = Duration.ofMillis(25);
    private static final int MAX_POLLS = 200;

    private SqlBatchLockTestSupport() {
    }

    static Mono<HeldRowLock> holdBatch(
            ConnectionFactory connectionFactory,
            String batchId,
            String initiativeId
    ) {
        return hold(
                connectionFactory,
                "SELECT id FROM reward_batches "
                        + "WHERE id = $1 AND initiative_id = $2 FOR UPDATE",
                batchId,
                initiativeId
        );
    }

    /**
     * Returns true only after PostgreSQL reports a backend blocked by the
     * supplied backend. It returns false after bounded polling rather than
     * failing before the test can release its locks.
     */
    static Mono<Boolean> blockedByWithin(
            ConnectionFactory connectionFactory,
            int blockingBackendPid
    ) {
        return blockedBy(connectionFactory, blockingBackendPid)
                .filter(Boolean::booleanValue)
                .repeatWhen(repeats -> repeats.delayElements(POLL_DELAY).take(MAX_POLLS))
                .next()
                .defaultIfEmpty(false);
    }

    private static Mono<HeldRowLock> hold(
            ConnectionFactory connectionFactory,
            String sql,
            Object... parameters
    ) {
        return Mono.from(connectionFactory.create())
                .flatMap(connection -> Mono.from(connection.beginTransaction())
                        .then(executeAndConsume(connection, sql, parameters))
                        .then(backendPid(connection))
                        .map(pid -> new HeldRowLock(connection, pid))
                        .onErrorResume(error -> Mono.from(connection.close())
                                .then(Mono.error(error))));
    }

    private static Mono<Void> executeAndConsume(
            Connection connection,
            String sql,
            Object... parameters
    ) {
        var statement = connection.createStatement(sql);
        for (int index = 0; index < parameters.length; index++) {
            statement.bind(index, parameters[index]);
        }
        return Flux.from(statement.execute())
                .flatMap(result -> result.map((row, metadata) -> row))
                .then();
    }

    private static Mono<Integer> backendPid(Connection connection) {
        return Flux.from(connection.createStatement("SELECT pg_backend_pid() AS pid").execute())
                .flatMap(result -> result.map((row, metadata) -> row.get("pid", Integer.class)))
                .single();
    }

    private static Mono<Boolean> blockedBy(
            ConnectionFactory connectionFactory,
            int blockingBackendPid
    ) {
        return Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                connection -> {
                    var statement = connection.createStatement("""
                            SELECT 1
                            FROM pg_stat_activity activity
                            WHERE activity.pid <> pg_backend_pid()
                              AND $1 = ANY(pg_blocking_pids(activity.pid))
                            """).bind(0, blockingBackendPid);
                    Publisher<Boolean> blockedBackends = Flux.from(statement.execute())
                            .flatMap(result -> result.map((row, metadata) -> true));
                    return Flux.from(blockedBackends).hasElements();
                },
                connection -> Mono.from(connection.close())
        );
    }

    static final class HeldRowLock {

        private final Connection connection;
        private final int backendPid;
        private final AtomicBoolean released = new AtomicBoolean();

        private HeldRowLock(Connection connection, int backendPid) {
            this.connection = connection;
            this.backendPid = backendPid;
        }

        int backendPid() {
            return backendPid;
        }

        Mono<Void> release() {
            if (!released.compareAndSet(false, true)) {
                return Mono.empty();
            }
            return Mono.from(connection.rollbackTransaction())
                    .onErrorResume(error -> Mono.empty())
                    .then(Mono.from(connection.close()));
        }
    }

}
