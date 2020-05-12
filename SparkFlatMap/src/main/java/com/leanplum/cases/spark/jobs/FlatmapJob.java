package com.leanplum.cases.spark.jobs;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.leanplum.cases.spark.AggregateJobInput;
import com.leanplum.cases.spark.schema.AggregationSchema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.util.ArrayList;
import java.util.List;

import static com.leanplum.cases.spark.Utils.startOfDay;
import static com.leanplum.cases.spark.jobs.ValuesFlatMapFunction.flattenedSchema;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * The flatMap version of the aggregation job.
 * The Dataset DSL is greatly simplified as the custom business logic for filtering, flattening and aggregation
 * is implemented in {@link ValuesFlatMapFunction}.
 * {@link ValuesFlatMapFunction} also provides an intermediate schema for the flatMap output ({@link ValuesFlatMapFunction#flattenedSchema()})
 *
 * Note: the amount and size of intermediate rows is now trivial and allows for having a single output parquet file.
 */
public class FlatmapJob implements SparkProtoJob<AggregateJobInput, Any> {

  public Any run(JavaSparkContext ctx, AggregateJobInput jobInput) {

    final List<String> pathsList = new ArrayList<>(jobInput.getInputPathsList());

    Preconditions.checkState(!pathsList.isEmpty());

    SparkSession.builder().sparkContext(ctx.sc()).getOrCreate()
      .read()
      .parquet(pathsList.toArray(new String[0]))
      .flatMap(new ValuesFlatMapFunction(), RowEncoder.apply(flattenedSchema()))
      .groupBy(
        // dimensions
        col(AggregationSchema.CUSTOMER_ID.name),
        startOfDay(col(AggregationSchema.DATE.name)).as(AggregationSchema.DATE.name),
        col(AggregationSchema.KIND.name),
        col(AggregationSchema.VALUE.name)
      )
      .agg(
        sum(col(AggregationSchema.COUNT.name)).as(AggregationSchema.COUNT.name),
        sum(col(AggregationSchema.OPERATIONS.name)).as(AggregationSchema.OPERATIONS.name)
      )
      .coalesce(1)
      .write()
      .mode("overwrite")
      .parquet(jobInput.getOutputPath());

    return Any.getDefaultInstance();
  }
}