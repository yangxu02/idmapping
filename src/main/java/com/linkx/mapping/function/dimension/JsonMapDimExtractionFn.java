package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class JsonMapDimExtractionFn extends DimensionExtractionFn {
    private final static ObjectMapper objectMapper = new ObjectMapper();
    private final static TypeReference type = new TypeReference<HashMap<String, Object>>() {};
    private final static Log logger = LogFactory.getLog(JsonPackingDimExtractionFn.class);

    private String key;

    @JsonCreator
    public JsonMapDimExtractionFn(@JsonProperty("key") String key) {
        this.key = key;
    }

    @Override
    public String apply(String input) {
        if (!Strings.isNullOrEmpty(input)) {
            try {
                Map<String, Object> content = objectMapper.readValue(input, type);
                Object val = content.get(key);
                if (null == val) return "";
                return "" + val;
            } catch (JsonMappingException e) {
                logger.error(input, e);
            } catch (JsonParseException e) {
                logger.error(input, e);
            } catch (IOException e) {
                logger.error(input, e);
            }
        }

        return "";
    }

    @Override
    public String apply(Row row) {
        return "";
    }

    @Override
    public String apply(Row row, String dim) {
        return apply(row.getDimensionVal(dim));
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }
}
