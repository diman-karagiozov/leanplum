package com.leanplum.cases.spark.schema;

import com.leanplum.cases.spark.Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.createArray;

public enum ContributionSchema {
  TYPE("type", Type.INT, false, "type of the billable action - API call 1, call 2, etc..."),
  USER("user_index", Type.INT, false, "index of updated user"),
  ACTIONS("actions", Type.INT, false, "number of billable actions for this user"),
  VALUES("values", createArray(ContribValuesSchema.createSchema()), true, "");

  ContributionSchema(String name, Type type, boolean optional, String doc) {
    this(name, Schema.create(type), optional, doc);
  }

  ContributionSchema(String name, Schema schema, boolean optional, String doc) {
    this.name = name;
    this.schema = schema;
    this.optional = optional;
    this.doc = doc;
  }

  public final String name;
  public final Schema schema;
  public final boolean optional;
  public final String doc;

  public Field createField() {
    return Utils.createField(name, schema, optional, doc);
  }

  public static Schema createSchema() {
    final List<Field> fields = Arrays.stream(ContributionSchema.values())
        .map(ContributionSchema::createField).collect(Collectors.toList());

    final Schema schema = Schema.createRecord("contrib", "", "", false);
    schema.setFields(fields);
    return schema;
  }
}
