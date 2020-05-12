package com.leanplum.cases.spark.schema;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;

public class BillingConstants {

  public enum BillableAction {
    TRACK(1),
    ADVANCE(2),
    SET_USER_ATTRIBUTES(3);

    private final int value;

    BillableAction(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }
  }

  public enum ValueType {
    EVENT(1),
    STATE(2),
    ATTRIBUTE(3),
    LOCALIZATION(4);

    private final int value;

    ValueType(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }
  }

  private final static Collection<Integer> ALLOWED_CONTRIB_TYPES = ImmutableSet.of(
    BillingConstants.BillableAction.TRACK.value(),
    BillingConstants.BillableAction.ADVANCE.value(),
    BillingConstants.BillableAction.SET_USER_ATTRIBUTES.value()
  );

  private final static Collection<Integer> ALLOWED_VALUES_TYPES = ImmutableSet.of(
    BillingConstants.ValueType.EVENT.value(), BillingConstants.ValueType.STATE.value(),
    BillingConstants.ValueType.ATTRIBUTE.value(), BillingConstants.ValueType.LOCALIZATION.value()
  );

  public static boolean contribTypeMatches(int contribType) {
    return ALLOWED_CONTRIB_TYPES.contains(contribType);
  }

  public static boolean valueTypeMatches(int valueType) {
    return ALLOWED_VALUES_TYPES.contains(valueType);
  }

  public static boolean isUserAttribute(int valueType) {
    return valueType == BillingConstants.ValueType.ATTRIBUTE.value() ||
      valueType == BillingConstants.ValueType.LOCALIZATION.value();
  }
}
