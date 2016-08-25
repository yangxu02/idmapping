package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.linkx.mapping.row.Row;

import java.math.BigDecimal;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class ScalingDimExtractionFn extends DimensionExtractionFn {

    private BigDecimal scale;

    @JsonCreator
    public ScalingDimExtractionFn(@JsonProperty("scale") BigDecimal scale) {
        this.scale = scale;
    }

    @Override
    public String apply(String input) {
        if (scale.intValue() == 1) return input;
        if (!Strings.isNullOrEmpty(input)) {
            try {
                BigDecimal decimal = new BigDecimal(input);
                decimal = decimal.multiply(scale);
                return decimal.longValue() + "";
            } catch (NumberFormatException e) {
            }
        }

        return "0";
    }

    @Override
    public String apply(Row row) {
        return "";
    }

    @Override
    public String apply(Row row, String val) {
        return apply(row.getDimensionVal(val));
    }

    @JsonProperty("scale")
    public BigDecimal getScale() {
        return scale;
    }
}
