package com.leanplum.cases.spark.jobs;

import com.leanplum.cases.spark.schema.AggregationSchema;
import com.leanplum.cases.spark.schema.BillingSchema;
import com.leanplum.cases.spark.schema.ContribValuesSchema;
import com.leanplum.cases.spark.schema.ContributionSchema;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.util.*;

import static com.leanplum.cases.spark.schema.BillingConstants.*;

/**
 * A flatMap function which transforms the double-nested input data into a flat structure with minimal set of columns.
 * In this case {@link FlatMapFunction} accepts a {@link Row} as input (one billing record) and transforms it to a collection
 * of rows with an intermediate flattened structure {@link ValuesFlatMapFunction#flattenedSchema()}.
 * <p>
 * Two nested FOR loops are used for accessing the nested data in a single row.
 * Custom business logic is implemented as part of {@link ValuesFlatMapFunction#call(org.apache.spark.sql.Row)} method
 * using simple IF statements.
 */
public class ValuesFlatMapFunction implements FlatMapFunction<Row, Row> {

  @Override
  public Iterator<Row> call(Row row) {
    // return no rows if there are no contributions
    if (row.isNullAt(row.schema().fieldIndex(BillingSchema.CONTRIB.name))) {
      return Collections.emptyIterator();
    }

    final Collection<GenericRowWithSchema> contribs = JavaConversions
      .asJavaCollection(row.getAs(BillingSchema.CONTRIB.name));

    final Collection<Row> outputRows = new ArrayList<>();

    for (GenericRowWithSchema c : contribs) {
      if (!contribTypeMatches(c.getAs(ContributionSchema.TYPE.name)) || c.isNullAt(c.schema().fieldIndex(ContributionSchema.VALUES.name))
      ) {
        // skip this row
        continue;
      }

      final Collection<GenericRowWithSchema> values = JavaConversions.asJavaCollection(c.getAs(ContributionSchema.VALUES.name));
      final int occurrencesSum = values.stream().mapToInt(v -> v.getAs(ContribValuesSchema.OCCURRENCES.name)).sum();
      for (GenericRowWithSchema v : values) {
        final int valueType = v.getAs(ContribValuesSchema.TYPE.name);
        if (valueTypeMatches(valueType)) {
          final List<Object> data = new ArrayList<>();
          // add dimensions
          data.add(row.getAs(BillingSchema.CUSTOMER_ID.name));
          data.add(row.getAs(BillingSchema.SERVER_TIME.name));
          data.add(v.getAs(ContribValuesSchema.TYPE.name));
          data.add(v.getAs(ContribValuesSchema.VALUE.name));

          // add occurrences
          data.add(v.getAs(ContribValuesSchema.OCCURRENCES.name));

          // add operations contrib
          final int accountedOperations = row.getAs(BillingSchema.OPERATIONS.name);
          final int occurrences = v.getAs(ContribValuesSchema.OCCURRENCES.name);
          final double operationsContrib = occurrences * accountedOperations;
          if (isUserAttribute(valueType)) {
            data.add(operationsContrib / occurrencesSum);
          } else {
            data.add(operationsContrib);
          }

          outputRows.add(RowFactory.create(data.toArray()));
        }
      }
    }

    return outputRows.iterator();
  }

  /**
   * Provides the intermediate flattened schema for further aggregation
   */
  static StructType flattenedSchema() {
    return new StructType()
      .add(AggregationSchema.CUSTOMER_ID.name, DataTypes.LongType, false)
      .add(AggregationSchema.DATE.name, DataTypes.LongType, false)
      .add(AggregationSchema.KIND.name, DataTypes.IntegerType, false)
      .add(AggregationSchema.VALUE.name, DataTypes.StringType, false)
      .add(AggregationSchema.COUNT.name, DataTypes.LongType, false)
      .add(AggregationSchema.OPERATIONS.name, DataTypes.DoubleType, false)
      ;
  }
}
