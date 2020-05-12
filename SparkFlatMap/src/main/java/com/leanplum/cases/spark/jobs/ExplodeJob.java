package com.leanplum.cases.spark.jobs;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.leanplum.cases.spark.AggregateJobInput;
import com.leanplum.cases.spark.schema.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;

import static com.leanplum.cases.spark.Utils.ref;
import static com.leanplum.cases.spark.Utils.startOfDay;
import static org.apache.spark.sql.functions.*;

/**
 * Simplified version of aggregation job using two explode() functions and one user-defined function ({@link SparkSession#udf()}).
 * Filtering and aggregation business logic is implemented as part of the Dataset DSL.
 * {@link ExplodeJob#customFilteringLogic()} for filtering DSL and {@link ExplodeJob#operationsContrib()} for aggregation.
 * <p>
 * Note: the output of the job is a set of 8 parquet files (...coalesce(8).write() ...).
 * It is overwhelming for one worker to merge all aggregated records.
 */
public class ExplodeJob implements SparkProtoJob<AggregateJobInput, Any> {

  private static final String CNT = "cnt";
  private static final String VALS = "vals";
  private static final String VALS_OCCURRENCES = "val_occurrences";

  @Override
  public Any run(JavaSparkContext ctx, AggregateJobInput jobInput) {

    final List<String> pathsList = new ArrayList<>(jobInput.getInputPathsList());

    Preconditions.checkState(!pathsList.isEmpty());

    final SparkSession sparkSession = SparkSession.builder().sparkContext(ctx.sc()).getOrCreate();

    // register user defined function (udf) which sums an array of integers
    sparkSession.udf().register(
      VALS_OCCURRENCES,
      (UDF1<WrappedArray<Integer>, Integer>) (array) ->
        JavaConversions.asJavaCollection(array).stream().mapToInt(Math::abs).sum()
      , DataTypes.IntegerType
    );

    sparkSession
      .read()
      .parquet(pathsList.toArray(new String[0]))
      .withColumn(CNT, explode(col(BillingSchema.CONTRIB.name)))
      .withColumn(VALS, explode(ref(CNT, ContributionSchema.VALUES.name)))
      .withColumn(VALS_OCCURRENCES, callUDF(VALS_OCCURRENCES,
        ref(CNT, ContributionSchema.VALUES.name, ContribValuesSchema.OCCURRENCES.name)))
      .filter(customFilteringLogic())
      .groupBy(
        col(BillingSchema.CUSTOMER_ID.name).as(AggregationSchema.CUSTOMER_ID.name),
        startOfDay(col(BillingSchema.SERVER_TIME.name)).as(AggregationSchema.DATE.name),
        ref(VALS, ContribValuesSchema.TYPE.name).as(AggregationSchema.KIND.name),
        ref(VALS, ContribValuesSchema.VALUE.name).as(AggregationSchema.VALUE.name)
      )
      .agg(
        sum(occurrences()).as(AggregationSchema.COUNT.name) /* occurrences */,
        sum(operationsContrib()).as(AggregationSchema.OPERATIONS.name) /* contribution to user operations */
      )

      .coalesce(8)
      .write()
      .mode("overwrite")
      .parquet(jobInput.getOutputPath());

    return Any.getDefaultInstance();
  }

  private static Column customFilteringLogic() {
    return contribType().equalTo(BillingConstants.BillableAction.TRACK.value()) /* Track */
      .and(valueType().equalTo(BillingConstants.ValueType.EVENT.value())) /* event */
      .and(ref(CNT, ContributionSchema.ACTIONS.name).gt(0)) /* only external actions*/
      .or(contribType().equalTo(BillingConstants.BillableAction.ADVANCE.value()) /* Advance */
        .and(valueType().equalTo(BillingConstants.ValueType.STATE.value())) /* state */
      )
      .or(contribType()
        .equalTo(BillingConstants.BillableAction.SET_USER_ATTRIBUTES.value()) /* SetUserAttributes */
        .and(valueType().equalTo(BillingConstants.ValueType.ATTRIBUTE.value()) /* attribute */
          .or(valueType().equalTo(BillingConstants.ValueType.LOCALIZATION.value())) /* localization */
        )
      );
  }

  private static Column operationsContrib() {
    return occurrences()
      .divide(
        when(valueType().equalTo(BillingConstants.ValueType.ATTRIBUTE.value()), ref(VALS_OCCURRENCES)).
          when(valueType().equalTo(BillingConstants.ValueType.LOCALIZATION.value()), ref(VALS_OCCURRENCES))
          .otherwise(1)
      )
      .multiply(col(BillingSchema.OPERATIONS.name));
  }

  private static Column occurrences() {
    return ref(VALS, ContribValuesSchema.OCCURRENCES.name);
  }

  private static Column contribType() {
    return ref(CNT, ContributionSchema.TYPE.name);
  }

  private static Column valueType() {
    return ref(VALS, ContribValuesSchema.TYPE.name);
  }
}