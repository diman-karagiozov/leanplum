package com.leanplum.cases.spark.schema;

import com.leanplum.cases.spark.Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum AggregationSchema {

  /**
   * Dimensions, used for drill-down
   **/
  DATE("date", Type.LONG, false, "Request server date in UTC, number of days since epoch"),
  CUSTOMER_ID("app_id", Type.LONG, false, "app id"),

  /**
   * Kind - event/state/attribute and its "name"
   */
  KIND("kind", Type.INT, false, "See Billing/Protos/src/main/proto/ingestion.proto"),
  VALUE("value", Type.STRING, false, "Name of the event, state or attribute"),

  /**
   * Values - occurrences and user operations
   */
  COUNT("cnt", Type.LONG, false, "Occurrences of the event/state/attribute"),
  OPERATIONS("ops", Type.DOUBLE, false, "Accounted operation related to event/state/attribute ");

  AggregationSchema(String name, Type type, boolean optional, String doc) {
    this(name, Schema.create(type), optional, doc);
  }

  AggregationSchema(String name, Schema schema, boolean optional, String doc) {
    this.name = name;
    this.schema = schema;
    this.optional = optional;
    this.doc = doc;
  }

  public final String name;
  public final Schema schema;
  public final boolean optional;
  public final String doc;

  public static Schema createSchema() {
    final List<Field> fields = Arrays.stream(AggregationSchema.values())
        .map(AggregationSchema::createField).collect(Collectors.toList());

    final Schema schema = Schema.createRecord("billing", "", "", false);
    schema.setFields(fields);
    return schema;
  }

  private Field createField() {
    return Utils.createField(name, schema, optional, doc);
  }
}
