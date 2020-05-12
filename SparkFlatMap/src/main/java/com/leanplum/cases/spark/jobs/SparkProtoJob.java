package com.leanplum.cases.spark.jobs;

import com.google.protobuf.Message;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public interface SparkProtoJob<Input extends Message, Output extends Message> extends Serializable {

  Output run(JavaSparkContext ctx, Input input) throws Exception;

}
