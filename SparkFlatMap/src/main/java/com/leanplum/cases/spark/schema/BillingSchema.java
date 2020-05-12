package com.leanplum.cases.spark.schema;

import com.leanplum.cases.spark.Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.createArray;

/**
 * Root schema for the billing parquets.
 */
public enum BillingSchema {
  REQUEST_ID("request_id", Type.STRING, true, "request id"),
  SERVER_TIME("server_time", Type.LONG, false, "server time of the api call"),
  CUSTOMER_ID("customer_id", Type.LONG, false, "app_id"),

  CONTRIB("contrib", createArray(ContributionSchema.createSchema()), true, ""),

  OPERATIONS("operations", Type.LONG, false, "Number of accounted operations");

  BillingSchema(String name, Type type, boolean optional, String doc) {
    this(name, Schema.create(type), optional, doc);
  }

  BillingSchema(String name, Schema schema, boolean optional, String doc) {
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
    final List<Field> fields = Arrays.stream(BillingSchema.values())
        .map(BillingSchema::createField).collect(Collectors.toList());

    final Schema schema = Schema.createRecord("billing", "", "", false);
    schema.setFields(fields);
    return schema;
  }

  private Field createField() {
    return Utils.createField(name, schema, optional, doc);
  }
}
