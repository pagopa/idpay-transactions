package it.gov.pagopa.idpay.transactions.repository;

import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction.Fields;
import it.gov.pagopa.idpay.transactions.service.RewardBatchServiceImpl;
import it.gov.pagopa.idpay.transactions.utils.AggregationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

@Slf4j
public class RewardTransactionSpecificRepositoryImpl implements RewardTransactionSpecificRepository {

  private final ReactiveMongoTemplate mongoTemplate;

  public RewardTransactionSpecificRepositoryImpl(ReactiveMongoTemplate mongoTemplate) {
    this.mongoTemplate = mongoTemplate;
  }

  @Override
  public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId,
      LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
    Criteria criteria = Criteria.where(RewardTransaction.Fields.idTrxIssuer).is(idTrxIssuer);
    if (userId != null) {
      criteria.and(RewardTransaction.Fields.userId).is(userId);
    }
    if (amountCents != null) {
      criteria.and(RewardTransaction.Fields.amountCents).is(amountCents);
    }
    if (trxDateStart != null && trxDateEnd != null) {
      criteria.and(RewardTransaction.Fields.trxDate)
          .gte(trxDateStart)
          .lte(trxDateEnd);
    } else if (trxDateStart != null) {
      criteria.and(RewardTransaction.Fields.trxDate)
          .gte(trxDateStart);
    } else if (trxDateEnd != null) {
      criteria.and(RewardTransaction.Fields.trxDate)
          .lte(trxDateEnd);
    }

    return mongoTemplate.find(
        Query.query(criteria)
            .with(getPageable(pageable)),
        RewardTransaction.class);
  }

  @Override
  public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart,
      LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
    Criteria criteria = Criteria
        .where(RewardTransaction.Fields.userId).is(userId)
        .and(RewardTransaction.Fields.trxDate)
        .gte(trxDateStart)
        .lte(trxDateEnd);
    if (amountCents != null) {
      criteria.and(RewardTransaction.Fields.amountCents).is(amountCents);
    }

    return mongoTemplate.find(
        Query.query(criteria)
            .with(getPageable(pageable)),
        RewardTransaction.class);
  }

  private Pageable getPageable(Pageable pageable) {
    if (pageable == null) {
      pageable = Pageable.unpaged();
    }
    return pageable;
  }

  private Criteria getCriteria(TrxFiltersDTO filters,
      String pointOfSaleId,
      String userId,
      String productGtin,
      boolean includeToCheckWithConsultable) {

    Criteria criteria = Criteria.where(RewardTransaction.Fields.merchantId)
        .is(filters.getMerchantId())
        .and(RewardTransaction.Fields.initiatives).is(filters.getInitiativeId());

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

    if (StringUtils.isNotBlank(filters.getStatus())) {
      criteria.and(RewardTransaction.Fields.status).is(filters.getStatus());
    } else {
      criteria.and(RewardTransaction.Fields.status)
          .in("CANCELLED", "REWARDED", "REFUNDED", "INVOICED");
    }

    if (filters.getRewardBatchId() != null) {
      criteria.and(Fields.rewardBatchId).is(filters.getRewardBatchId());
    }

    if (filters.getTrxCode() != null) {
      criteria.and(Fields.trxCode)
              .regex(".*" + Pattern.quote(filters.getTrxCode()) + ".*", "i");
    }

    if (filters.getRewardBatchTrxStatus() != null) {
      if (includeToCheckWithConsultable
          && filters.getRewardBatchTrxStatus() == RewardBatchTrxStatus.CONSULTABLE) {

        criteria.and(Fields.rewardBatchTrxStatus)
            .in(RewardBatchTrxStatus.CONSULTABLE.name(),
                RewardBatchTrxStatus.TO_CHECK.name());

      } else {
        criteria.and(Fields.rewardBatchTrxStatus)
            .is(filters.getRewardBatchTrxStatus().name());
      }
    }

    return criteria;
  }


  @Override
  public Flux<RewardTransaction> findByFilter(TrxFiltersDTO filters,
      String userId,
      boolean includeToCheckWithConsultable,
      Pageable pageable) {
    Criteria criteria = getCriteria(filters, filters.getPointOfSaleId(), userId, null,
        includeToCheckWithConsultable);
    return mongoTemplate.find(Query.query(criteria).with(getPageable(pageable)),
        RewardTransaction.class);
  }

  @Override
  public Flux<RewardTransaction> findByFilter(String rewardBatchId, String initiativeId,
      List<RewardBatchTrxStatus> statusList) {
    Criteria criteria = getCriteria(rewardBatchId, initiativeId, statusList);
    return mongoTemplate.find(Query.query(criteria), RewardTransaction.class);
  }

  private Criteria getCriteria(String rewardBatchId, String initiativeId,
      List<RewardBatchTrxStatus> statusList) {
    return Criteria.where(RewardTransaction.Fields.rewardBatchId).is(rewardBatchId)
        .and(RewardTransaction.Fields.initiatives).is(initiativeId)
        .and(Fields.rewardBatchTrxStatus).in(statusList);
  }


  @Override
  public Mono<RewardTransaction> findTransaction(String merchantId, String transactionId) {
    Criteria criteria = Criteria
        .where(Fields.merchantId).is(merchantId)
        .and(Fields.id).is(transactionId)
        .and(Fields.status)
        .in(SyncTrxStatus.REWARDED, SyncTrxStatus.REFUNDED, SyncTrxStatus.INVOICED);
    return mongoTemplate.findOne(Query.query(criteria), RewardTransaction.class);
  }


  @Override
  public Mono<Long> getCount(TrxFiltersDTO filters,
      String pointOfSaleId,
      String productGtin,
      String userId,
      boolean includeToCheckWithConsultable) {

    Criteria criteria = getCriteria(filters, pointOfSaleId, userId, productGtin,
        includeToCheckWithConsultable);
    return mongoTemplate.count(Query.query(criteria), RewardTransaction.class);
  }

  @Override
  public Mono<RewardTransaction> findOneByInitiativeId(String initiativeId) {
    Criteria criteria = getCriteria(initiativeId);
    return mongoTemplate.findOne(Query.query(criteria), RewardTransaction.class);
  }

  private static Criteria getCriteria(String initiativeId) {
    return Criteria.where(Fields.initiatives).is(initiativeId);
  }

  @Override
  public Mono<Void> removeInitiativeOnTransaction(String trxId, String initiativeId) {
    Criteria criteria = Criteria.where(RewardTransaction.Fields.id).is(trxId);
    return mongoTemplate.updateFirst(Query.query(criteria),
            new Update()
                .pull(RewardTransaction.Fields.initiatives, initiativeId)
                .unset("%s.%s".formatted(RewardTransaction.Fields.rewards, initiativeId))
                .unset("%s.%s".formatted(RewardTransaction.Fields.initiativeRejectionReasons,
                    initiativeId)),
            RewardTransaction.class)
        .then();
  }

  @Override
  public Flux<RewardTransaction> findByInitiativesWithBatch(String initiativeId, int batchSize) {
    Query query = Query.query(Criteria.where(RewardTransaction.Fields.initiatives).is(initiativeId))
        .cursorBatchSize(batchSize);
    return mongoTemplate.find(query, RewardTransaction.class);
  }

  @Override

  public Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId) {
    Criteria criteria = Criteria.where(RewardTransaction.Fields.userId).is(userId)
        .and(Fields.initiatives).in(initiativeId);

    return mongoTemplate.find(Query.query(criteria), RewardTransaction.class);
  }


  @Override
  public Mono<Void> rewardTransactionsByBatchIdAndInitiativeId(String batchId, String initiativeId) {
    Criteria batchCriteria = Criteria.where(Fields.rewardBatchId).is(batchId);
    Criteria samplingBatchCriteria = Criteria.where(Fields.rewardBatchId).is(batchId)
        .and(Fields.rewardBatchTrxStatus).ne(RewardBatchTrxStatus.SUSPENDED)
        .and(Fields.initiatives).in(initiativeId);

    Mono<Long> totalMono = mongoTemplate.updateMulti(
            Query.query(batchCriteria),
            new Update().set(Fields.status, SyncTrxStatus.REWARDED),
            RewardTransaction.class
        )
        .map(UpdateResult::getModifiedCount);

    return totalMono
        .filter(total -> total > 0)
        .flatMap(total -> {
          int toVerify = (int) Math.ceil(total * 0.15);

          Query sampleQuery = Query.query(samplingBatchCriteria);
          sampleQuery.with(Sort.by(Sort.Direction.ASC, Fields.samplingKey));
          sampleQuery.limit(toVerify);
          sampleQuery.fields().include(Fields.id);

          Mono<List<String>> idsToVerifyMono = mongoTemplate
              .find(sampleQuery, RewardTransaction.class)
              .map(RewardTransaction::getId)
              .collectList();

          return idsToVerifyMono
              .filter(ids -> !ids.isEmpty())
              .flatMap(idsToVerify -> {
                Criteria toVerifyCriteria = Criteria.where(Fields.id).in(idsToVerify);

                return mongoTemplate.updateMulti(
                        Query.query(toVerifyCriteria),
                        new Update().set(Fields.rewardBatchTrxStatus, RewardBatchTrxStatus.TO_CHECK),
                        RewardTransaction.class
                    )
                    .then();
              });
        })
        .then();
  }

  @Override
  public Mono<Long> sumSuspendedAccruedRewardCents(String initiativeId, String rewardBatchId) {

    MatchOperation match = Aggregation.match(
        Criteria.where(Fields.rewardBatchId).is(rewardBatchId)
            .and(Fields.rewardBatchTrxStatus).is(RewardBatchTrxStatus.SUSPENDED)
            .and(Fields.initiatives).in(initiativeId)
    );

    Aggregation agg = Aggregation.newAggregation(
        match,
        Aggregation.project().andExpression("objectToArray(rewards)").as("rewardsArray"),
        Aggregation.unwind("rewardsArray"),
        Aggregation.group().sum("rewardsArray.v.accruedRewardCents").as("total")
    );

    return mongoTemplate
        .aggregate(agg, RewardTransaction.class, RewardBatchServiceImpl.TotalAmount.class)
        .next()
        .map(RewardBatchServiceImpl.TotalAmount::getTotal)
        .defaultIfEmpty(0L);
  }

  @Override
  public Mono<RewardTransaction> updateStatusAndReturnOld(
          String initiativeId,
          String batchId,
          String trxId,
          RewardBatchTrxStatus newStatus,
          ReasonDTO reasons,
          String batchMonth,
          ChecksError checksError
  ) {

      Criteria base = Criteria.where(Fields.id).is(trxId)
              .and(Fields.rewardBatchId).is(batchId)
              .and(Fields.initiatives).in(initiativeId);

      Query findQuery = Query.query(base);
      findQuery.fields().include(Fields.rewardBatchTrxStatus);

      return Mono.defer(() ->
              mongoTemplate.findOne(findQuery, RewardTransaction.class)
                      .flatMap(current -> {
                          if (current == null) {
                              log.info("Transaction not found for id {} and reward batch {}", trxId, batchId);
                              return Mono.empty();
                          }

                          RewardBatchTrxStatus currentStatus = current.getRewardBatchTrxStatus();
                          Update update = createUpdate(currentStatus, newStatus, reasons, batchMonth, checksError);

                          Criteria cond = Criteria.where(Fields.id).is(trxId)
                                  .and(Fields.rewardBatchId).is(batchId)
                                  .and(Fields.rewardBatchTrxStatus).is(currentStatus);

                          return mongoTemplate.findAndModify(
                                  Query.query(cond),
                                  update,
                                  FindAndModifyOptions.options().returnNew(false).upsert(false),
                                  RewardTransaction.class
                          );
                      })
      );
  }

  private Update createUpdate(
          RewardBatchTrxStatus currentStatus,
          RewardBatchTrxStatus newStatus,
          ReasonDTO reasons,
          String batchMonth,
          ChecksError checksError
  ) {
      Update update = new Update()
              .set(Fields.rewardBatchTrxStatus, newStatus)
              .set(Fields.rewardBatchLastMonthElaborated, batchMonth);

      if (checksError != null) {
          update.set(RewardTransaction.Fields.checksError, checksError);
      } else {
          update.unset(RewardTransaction.Fields.checksError);
      }

      if (reasons == null) {
          update.unset(RewardTransaction.Fields.rewardBatchRejectionReason);
      } else if (Objects.equals(currentStatus, newStatus)) {
          update.push(RewardTransaction.Fields.rewardBatchRejectionReason).value(reasons);
      } else {
          update.set(RewardTransaction.Fields.rewardBatchRejectionReason, List.of(reasons));
      }

      return update;
  }

  @Override
  public Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(int pageSize) {
    Pageable pageable = PageRequest.of(0, pageSize);

    Criteria criteria = Criteria
        .where(Fields.status).is(SyncTrxStatus.INVOICED)
        .and(Fields.rewardBatchId).isNull();

    Query query = Query.query(criteria).with(pageable);

    return mongoTemplate.find(query, RewardTransaction.class);
  }

  @Override
  public Mono<RewardTransaction> findInvoicedTrxByIdWithoutBatch(String initiativeId, String merchantId, String trxId) {
    Criteria criteria = Criteria.where(Fields.id).is(trxId)
        .and(Fields.status).is(SyncTrxStatus.INVOICED)
        .and(Fields.rewardBatchId).isNull()
        .and(Fields.initiatives).in(initiativeId)
        .and(Fields.merchantId).is(merchantId);

    Query query = Query.query(criteria);

    return Mono.from(mongoTemplate.findOne(query, RewardTransaction.class));
  }

  @Override
  public Flux<FranchisePointOfSaleDTO> findDistinctFranchiseAndPosByRewardBatchId(
          String rewardBatchId, String merchantId) {

    MatchOperation match = Aggregation.match(
        Criteria.where(RewardTransaction.Fields.rewardBatchId).is(rewardBatchId)
                .and(Fields.merchantId).is(merchantId)
    );

    GroupOperation group = Aggregation.group(
        RewardTransaction.Fields.franchiseName,
        RewardTransaction.Fields.pointOfSaleId
    );

    ProjectionOperation project = Aggregation.project()
        .and("_id." + RewardTransaction.Fields.franchiseName).as("franchiseName")
        .and("_id." + RewardTransaction.Fields.pointOfSaleId).as("pointOfSaleId");

    Aggregation aggregation = Aggregation.newAggregation(
        match,
        group,
        project
    );

    return mongoTemplate.aggregate(
        aggregation,
        RewardTransaction.class,
        FranchisePointOfSaleDTO.class
    );
  }

    public Mono<RewardTransaction> findTransactionInBatch (String initiativeId, String merchantId, String
    rewardBatchId, String transactionId){
      Criteria criteria = Criteria
          .where(RewardTransaction.Fields.id).is(transactionId)
          .and(Fields.initiatives).in(initiativeId)
          .and(RewardTransaction.Fields.merchantId).is(merchantId)
          .and(RewardTransaction.Fields.rewardBatchId).is(rewardBatchId);

      return mongoTemplate.findOne(Query.query(criteria), RewardTransaction.class);
    }
  }
