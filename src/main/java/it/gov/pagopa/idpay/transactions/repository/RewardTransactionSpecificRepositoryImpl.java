package it.gov.pagopa.idpay.transactions.repository;

import static it.gov.pagopa.idpay.transactions.utils.AggregationConstants.FIELD_PRODUCT_NAME;
import static it.gov.pagopa.idpay.transactions.utils.AggregationConstants.FIELD_STATUS;

import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction.Fields;
import it.gov.pagopa.idpay.transactions.utils.AggregationConstants;
import java.util.Optional;

import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Slf4j
public class RewardTransactionSpecificRepositoryImpl implements RewardTransactionSpecificRepository{
    private final ReactiveMongoTemplate mongoTemplate;

    public RewardTransactionSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.idTrxIssuer).is(idTrxIssuer);
        if (userId != null) {criteria.and(RewardTransaction.Fields.userId).is(userId);}
        if (amountCents != null) {criteria.and(RewardTransaction.Fields.amountCents).is(amountCents);}
        if(trxDateStart != null && trxDateEnd != null){
            criteria.and(RewardTransaction.Fields.trxDate)
                .gte(trxDateStart)
                .lte(trxDateEnd);
        }else if(trxDateStart != null){
            criteria.and(RewardTransaction.Fields.trxDate)
                    .gte(trxDateStart);
        }else if(trxDateEnd != null){
            criteria.and(RewardTransaction.Fields.trxDate)
                    .lte(trxDateEnd);
        }

        return mongoTemplate.find(
                Query.query(criteria)
                        .with(getPageable(pageable)),
                RewardTransaction.class);
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        Criteria criteria = Criteria
                .where(RewardTransaction.Fields.userId).is(userId)
                .and(RewardTransaction.Fields.trxDate)
                .gte(trxDateStart)
                .lte(trxDateEnd);
        if(amountCents != null){criteria.and(RewardTransaction.Fields.amountCents).is(amountCents);}

        return mongoTemplate.find(
                Query.query(criteria)
                        .with(getPageable(pageable)),
                RewardTransaction.class);
    }

    private Pageable getPageableTrx(Pageable pageable) {
        if (pageable == null || pageable.getSort().isUnsorted()) {
           return PageRequest.of(0, 10, Sort.by("elaborationDateTime").descending());
        }
        return PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
    }

    private Pageable getPageable(Pageable pageable){
        if (pageable == null) {
            pageable = Pageable.unpaged();
        }
        return pageable;
    }

    private Criteria getCriteria(String merchantId, String initiativeId, String pointOfSaleId, String userId, String status, String productGtin) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.merchantId).is(merchantId)
                .and(RewardTransaction.Fields.initiatives).is(initiativeId);
        if (userId != null) {
            criteria.and(RewardTransaction.Fields.userId).is(userId);
        }
        if (pointOfSaleId != null) {
            criteria.and(Fields.pointOfSaleId).is(pointOfSaleId);
        }
        if (StringUtils.isNotBlank(productGtin)) {
          criteria.and(AggregationConstants.FIELD_PRODUCT_GTIN)
              .regex(".*" + Pattern.quote(productGtin) + ".*", "i");
        }
        if (StringUtils.isNotBlank(status)) {
            criteria.and(RewardTransaction.Fields.status).is(status);
        } else {
            criteria.and(RewardTransaction.Fields.status).in("CANCELLED", "REWARDED", "REFUNDED");
        }
        return criteria;
    }

    @Override
    public Flux<RewardTransaction> findByFilter(String merchantId, String initiativeId, String userId, String status, Pageable pageable){
        Criteria criteria = getCriteria(merchantId, initiativeId, null, userId, status, null);
        return mongoTemplate.find(Query.query(criteria).with(getPageable(pageable)), RewardTransaction.class);
    }

    @Override
    public Flux<RewardTransaction> findByFilterTrx(String merchantId, String initiativeId, String pointOfSaleId, String userId, String productGtin, String status, Pageable pageable){
        Criteria criteria = getCriteria(merchantId, initiativeId, pointOfSaleId, userId, status, productGtin);

        Pageable mappedPageable = mapSort(pageable);

        Aggregation aggregation = buildAggregation(criteria, mappedPageable);

        if (aggregation != null) {
            return mongoTemplate.aggregate(aggregation, RewardTransaction.class, RewardTransaction.class);
        } else {
            return mongoTemplate.find(
                Query.query(criteria).with(getPageableTrx(mappedPageable)),
                RewardTransaction.class);
        }
    }

    @Override
    public Mono<RewardTransaction> findTransaction(
            String merchantId, String pointOfSaleId, String transactionId) {
        Criteria criteria = Criteria
                .where(Fields.merchantId).is(merchantId)
                .and(Fields.pointOfSaleId).is(pointOfSaleId)
                .and(Fields.id).is(transactionId)
                .and(Fields.status).in(SyncTrxStatus.REWARDED,SyncTrxStatus.REFUNDED);
        return mongoTemplate.findOne(Query.query(criteria), RewardTransaction.class);
    }

    private Pageable mapSort(Pageable pageable) {
       Pageable basePageable = getPageableTrx(pageable);

        Sort mappedSort = Sort.by(
            basePageable.getSort().stream()
                .map(order -> {
                    if ("updateDate".equalsIgnoreCase(order.getProperty())) {
                        return new Sort.Order(order.getDirection(), "elaborationDateTime");
                    }
                    if ("productName".equalsIgnoreCase(order.getProperty())) {
                        return new Sort.Order(order.getDirection(), FIELD_PRODUCT_NAME);
                    }
                    return order;
                })
                .toList()
        );

        return PageRequest.of(basePageable.getPageNumber(), basePageable.getPageSize(), mappedSort);
    }

    private Aggregation buildAggregation(Criteria criteria, Pageable pageable) {
        Sort sort = pageable.getSort();

        if (isSortedBy(sort, FIELD_STATUS)) {
            return buildStatusAggregation(criteria, pageable);
        }
        return null;
    }

    private boolean isSortedBy(Sort sort, String property) {
        return sort.stream().anyMatch(order -> order.getProperty().equalsIgnoreCase(property));
    }

    private Aggregation buildStatusAggregation(Criteria criteria, Pageable pageable) {
        Sort.Direction direction = getSortDirection(pageable, FIELD_STATUS);

        return Aggregation.newAggregation(
            Aggregation.addFields()
                .addField("statusRank")
                .withValue(
                    ConditionalOperators.switchCases(
                        ConditionalOperators.Switch.CaseOperator.when(ComparisonOperators.valueOf(FIELD_STATUS).equalToValue("CANCELLED")).then(1),
                        ConditionalOperators.Switch.CaseOperator.when(ComparisonOperators.valueOf(FIELD_STATUS).equalToValue("REWARDED")).then(2),
                        ConditionalOperators.Switch.CaseOperator.when(ComparisonOperators.valueOf(FIELD_STATUS).equalToValue("REFUNDED")).then(3)
                    ).defaultTo(99)
                ).build(),
            Aggregation.match(criteria),
            Aggregation.sort(Sort.by(direction, "statusRank")),
            Aggregation.skip(pageable.getOffset()),
            Aggregation.limit(pageable.getPageSize())
        );
    }

    private Sort.Direction getSortDirection(Pageable pageable, String property) {
        return Optional.ofNullable(pageable.getSort().getOrderFor(property))
            .map(Sort.Order::getDirection)
            .orElse(Sort.Direction.ASC);
    }

    @Override
    public Mono<Long> getCount(String merchantId, String initiativeId, String pointOfSaleId, String productGtin, String userId, String status) {
        Criteria criteria = getCriteria(merchantId, initiativeId, pointOfSaleId, userId, status, productGtin);

        return mongoTemplate.count(Query.query(criteria), RewardTransaction.class);
    }

    @Override
    public Mono<RewardTransaction> findOneByInitiativeId(String initiativeId) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.initiatives).is(initiativeId);
        return mongoTemplate.findOne(Query.query(criteria), RewardTransaction.class);
    }

    @Override
    public Mono<Void> removeInitiativeOnTransaction(String trxId, String initiativeId) {
        Criteria criteria = Criteria.where(RewardTransaction.Fields.id).is(trxId);
        return mongoTemplate.updateFirst(Query.query(criteria),
                new Update()
                        .pull(RewardTransaction.Fields.initiatives, initiativeId)
                        .unset("%s.%s".formatted(RewardTransaction.Fields.rewards, initiativeId))
                        .unset("%s.%s".formatted(RewardTransaction.Fields.initiativeRejectionReasons, initiativeId)),
                RewardTransaction.class)
                .then();
    }

    @Override
    public Flux<RewardTransaction> findByInitiativesWithBatch(String initiativeId, int batchSize){
        Query query = Query.query(Criteria.where(RewardTransaction.Fields.initiatives).is(initiativeId)).cursorBatchSize(batchSize);
        return mongoTemplate.find(query, RewardTransaction.class);
    }
}
