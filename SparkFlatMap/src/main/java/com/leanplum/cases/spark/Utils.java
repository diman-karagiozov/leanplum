package com.leanplum.cases.spark;

import org.apache.avro.Schema;
import org.apache.spark.sql.Column;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Utils {

  public static Schema.Field createField(String name, Schema schema, boolean optional, String doc) {
    if (optional) {
      schema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }
    return new Schema.Field(name, schema, doc, (Object) null);
  }

  public static Column startOfDay(Column col) {
    return to_date(from_unixtime(col.divide(1000)));
  }

  public static Column ref(String... names) {
    return col(String.join(".", names));
  }
}
