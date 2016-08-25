package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * pack all key value into a string
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class JsonPackingDimExtractionFn extends DimensionExtractionFn {
    private final static ObjectMapper objectMapper = new ObjectMapper();
    private final static Log logger = LogFactory.getLog(JsonPackingDimExtractionFn.class);

    private Set<String> filters = null;
    private Filter filter;

    @JsonCreator
    public JsonPackingDimExtractionFn(@JsonProperty("filters") Set<String> filters) {
        this.filters = filters;
        this.filter = new Filter(filters);
    }

    public JsonPackingDimExtractionFn() {
        this.filter = new Filter(filters);
    }

    @JsonProperty("filters")
    public Set<String> getFilters() {
        return filters;
    }

    @Override
    public String apply(String input) {
        return "";
    }

    @Override
    public String apply(Row row) {
        if (null == row || row.getData() == null) return "";
        try {
            Map<String, String> mapBasedRow = (Map<String, String>) row.getData();
            return objectMapper.writeValueAsString(Maps.filterKeys(mapBasedRow, filter));
        } catch (JsonProcessingException e) {
            logger.error("", e);
        }
        return "";
    }

    @Override
    public String apply(Row row, String dim) {
        return apply(row);
    }
}

class Filter implements Predicate<String> {
    final Set<String> keys;

    Filter(Set<String> keys) {
        this.keys = keys;
    }

    @Override
    public boolean apply(@Nullable String input) {
        if (null == keys || keys.isEmpty()) return true;

        if (Strings.isNullOrEmpty(input)) {
            return false;
        }

        return !keys.contains(input);
    }
}