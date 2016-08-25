package com.linkx.mapping.guice;
/**
 * Created by yangxu on 1/20/15.
 */

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import com.linkx.mapping.context.MultipleContext;
import com.linkx.mapping.filter.AndExpression;
import com.linkx.mapping.filter.Expression;
import com.linkx.mapping.filter.Expressions;
import com.linkx.mapping.filter.OrExpression;
import com.linkx.mapping.filter.SimpleExpression;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.Input;
import com.linkx.mapping.function.dimension.DimensionExtractionFn;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MultipleContextProvider implements Provider<Context> {

    private static Logger logger = Logger.getLogger(MultipleContextProvider.class);

    @Override
    public Context get() {
        String cfgFile = System.getProperty("dmp.mapping.conf", "mapping.conf");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream stream = classLoader.getResourceAsStream(cfgFile);
        Input input = null;
        Map<Expression, Context> contextMap = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String debug = System.getProperty("dmp.mapping.debug.enabled", "false");
        if ("true".equals(debug)) {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
            }
        }


        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            String line;
            Map<String, List<String>> configs = new HashMap<>();

            collectConfigs(reader, configs);

            for (Map.Entry<String, List<String>> entry : configs.entrySet()) {
                if (entry.getKey().startsWith("input")) {
                    input = parseInput(entry.getValue(), classLoader, mapper);
                } else if (entry.getKey().startsWith("context")) {
                    parseContext(entry.getValue(), classLoader, mapper,
                            input, contextMap);
                }
            }

            reader.close();
            stream.close();
        } catch (JsonMappingException e) {
            logger.error("", e);
        } catch (JsonParseException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }

        Preconditions.checkNotNull(input, "Input description loading error with file:" + cfgFile);
        Preconditions.checkArgument(!contextMap.isEmpty(), "Mapping rules loading error with file:" + cfgFile);

        logger.info("successfully loading Mapping Context from file:" + cfgFile);

        MultipleContext context = new MultipleContext(ImmutableMap.copyOf(contextMap));
        context.setInput(input);

        return context;
    }

    void collectConfigs(BufferedReader reader, Map<String, List<String>> configs) throws IOException {
        String line;
        int i = 0;
        List<String> lines = null;
        while (null != (line = reader.readLine())) {
            line = line.trim();
            if (line.isEmpty()
                    || line.charAt(0) == '#') {
                continue;
            }
            if (line.charAt(0) == '[') {
                String section = line.substring(1, line.length() - 1);
                lines = new ArrayList<>();
                configs.put(section + "_" + i++, lines);
                continue;
            }
            if (null != lines) {
                lines.add(line);
            }
        }
    }

    Input parseInput(List<String> lines, ClassLoader loader, ObjectMapper mapper) throws IOException {
        Input input = null;

        for (String line : lines) {
            if (line.startsWith("include")) {
                int s = "include".length();
                while (' ' == line.charAt(s++));
                String file = line.substring(s - 1);
                file = CharMatcher.anyOf(" \"'").trimFrom(file);
                InputStream stream = loader.getResourceAsStream(file);
                input = mapper.readValue(stream, Input.class);
                stream.close();
            }
        }

        return input;
    }

    void parseContext(List<String> lines, ClassLoader loader,
                      ObjectMapper mapper, Input input,
                      Map<Expression, Context> contextMap) throws IOException {
        Expression expression = null;
        Context context = null;
        Map<String, String> props = new HashMap<>();
        for (String line : lines){
            int s = line.indexOf(' ');
            String key = line.substring(0, s);
            while (' ' == line.charAt(s++));
            String val = line.substring(s - 1).trim();
            val = CharMatcher.anyOf(" \"'").trimFrom(val);
            props.put(key, val);
        }

        String expr = props.get("expr");
        if (!Strings.isNullOrEmpty(expr)) {
            expression = Expressions.create(expr);
            mapper.enableDefaultTypingAsProperty(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE, "type");
            if (null != expression) {
                if (expression instanceof SimpleExpression) {
                    String fn = props.get("fn." + expression.getKey());
                    ((SimpleExpression) expression).setFn(mapper.readValue(fn, DimensionExtractionFn.class));
                } else {
                    List<Expression> expressions = Collections.EMPTY_LIST;
                    if (expression instanceof AndExpression) {
                        expressions = ((AndExpression) expression).getExpressions();
                    } else if (expression instanceof OrExpression) {
                        expressions = ((OrExpression) expression).getExpressions();
                    }
                    for (Expression simpleExpr : expressions) {
                        String fn = props.get("fn." + simpleExpr.getKey());
                        ((SimpleExpression) simpleExpr).setFn(mapper.readValue(fn, DimensionExtractionFn.class));
                    }
                }
            }
            mapper.disableDefaultTyping();
        }
        String include = props.get("include");
        if (!Strings.isNullOrEmpty(include)) {
            logger.info("parse context input from " + include);
            InputStream stream = loader.getResourceAsStream(include);
            context = mapper.readValue(stream, Context.class);
            stream.close();
        }

        if (null != expression && null != context) {
            context.setInput(input);
            contextMap.put(expression, context);
        }
    }
}
