package com.leanplum.cases.spark.schema;

import com.leanplum.cases.spark.Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Schema of the values for each contribution
 */
public enum ContribValuesSchema {
  TYPE("type", Type.INT, false, "type of the value - event, state, attribute, etc."),
  VALUE("Value", Type.STRING, false, "string literal value"),
  OCCURRENCES("occurrences", Type.INT, false, "occurrences of this value.");

  ContribValuesSchema(String name, Type type, boolean optional, String doc) {
    this(name, Schema.create(type), optional, doc);
  }

  ContribValuesSchema(String name, Schema schema, boolean optional, String doc) {
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
    final Schema schema = Schema.createRecord("values", "", "", false);
    schema.setFields(Arrays
        .stream(ContribValuesSchema.values())
        .map(ContribValuesSchema::createField)
        .collect(Collectors.toList())
    );
    return schema;
  }
}
