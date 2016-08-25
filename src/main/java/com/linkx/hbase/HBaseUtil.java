package com.linkx.hbase;
/**
 * Created by yangxu on 10/31/14.
 */

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class HBaseUtil {

    private static Logger logger = Logger.getLogger(HBaseUtil.class);


    private final static String HBASE_TABLE_REGIONS = "hbase.yeahmobi.table.regions";
    private final static String HBASE_AUTH_FREQUENCY = "hbase.yeahmobi.auth.frequency";
    private final static String HBASE_AUTH_ENABLED = "hbase.yeahmobi.auth.enabled";
    private final static String HBASE_TABLE_ROWKEY_GENERATOR = "hbase.yeahmobi.table.rowkey.generator";
    private final static String HBASE_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
    private final static String HBASE_KEYTAB_PATH = "hbase.keytab.path";
    private final static ImmutableMap<String, String> tableRegions;
    private static final RowKeyGenerator rowKeyGenerator;
    private static final String principal;
    private static final String keytab;
    private static AuthTimer authTimer;
    private static boolean authEnabled;
    private static final Configuration conf;

    static {
        // set global hbase variables
        conf = HBaseConfiguration.create();
        conf.addResource("classpath:hbase-site.xml");

        PropertiesConfiguration cfg = new PropertiesConfiguration();
        try {
            String cfgFile = System.getProperty("ym.hbase.utils.conf", "config.properties");
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream stream = classLoader.getResourceAsStream(cfgFile);
            cfg.load(stream, "utf-8");
        } catch (ConfigurationException e) {
            logger.error("", e);
        }

        // set project level hbase variables
        // read table region config from xml file
        String regions = cfg.getString(HBASE_TABLE_REGIONS, "");
        if (!Strings.isNullOrEmpty(regions) && regions.contains(":")) {
            tableRegions = ImmutableMap.copyOf(Splitter.on(',')
                    .omitEmptyStrings()
                    .trimResults()
                    .withKeyValueSeparator(':').split(regions));
        } else {
            tableRegions = ImmutableMap.of();
        }
        String generator = cfg.getString(HBASE_TABLE_ROWKEY_GENERATOR, "hash");
        rowKeyGenerator = RowKeyGenerator.fromString(generator, "hash");

        authEnabled = cfg.getBoolean(HBASE_AUTH_ENABLED, true);

        principal = cfg.getString(HBASE_MASTER_KERBEROS_PRINCIPAL);
        keytab = cfg.getString(HBASE_KEYTAB_PATH);

        try {
            authTimer = AuthTimer.valueOf("AUTHON" +
                    cfg.getString(HBASE_AUTH_FREQUENCY, "halfday").toUpperCase());
        } catch (Exception e) {
            authTimer = AuthTimer.AUTHONHALFDAY;
            logger.error("", e);
        }

        if (authEnabled) {
            authTimer.start();
        }

        logger.info("tableRegions=" + regions
                        + ",rowKeyGenerator="  + rowKeyGenerator
                        + ",principal="  + principal
                        + ",keytab="  + keytab
                        + ",authEnabled="  + authEnabled
                        + ",authTimer="  + authTimer
        );

    }

    public static ImmutableMap<String, String> getTableRegions() {
        return tableRegions;
    }

    public static RowKeyGenerator getRowKeyGenerator() {
        return rowKeyGenerator;
    }

    public static Configuration getConf() {
        return conf;
    }

    public static boolean needAuthOnQuery() {
        return AuthTimer.AUTHONQUERY == authTimer;
    }

    public static boolean auth() {
        if (!authEnabled) return true;
        return auth(conf);
    }

    public static boolean auth(Configuration conf) {
        if (!authEnabled) return true;
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(
                    principal,
                    keytab
            );
            return true;
        } catch (IOException e) {
            logger.info("auth failed:principal=" + principal
                    + ",keytab=" + keytab,
                    e);
        }
        return false;
    }

    public static HConnection getConnection() {
        try {
            if (!authEnabled  // no need to auth
                    || !needAuthOnQuery() // no need to auth on each query
                    || auth(conf, principal, keytab) // auth pass
                    ) {
                return HConnectionManager.createConnection(conf);
            }
        } catch (IOException e) {
            logger.error("", e);
        }
        return null;
    }

    public static void releaseConnection(HConnection hConnection) {
        if (null == hConnection) return;
        try {
            hConnection.close();
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public static boolean auth(Configuration conf, String principal, String keytab) {
        if (!authEnabled) return true;
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(
                    principal,
                    keytab
            );
            return true;
        } catch (IOException e) {
            logger.info("auth failed:principal=" + principal
                    + ",keytab=" + keytab,
                    e);
        }
        return false;
    }

    public enum AuthTimer {
        AUTHONQUERY(-1) {
            void start() {}
        },
        AUTHONINIT(-1) {
            void start() {
                auth(conf, principal, keytab);
            }
        },
        AUTHONMINUTE(TimeUnit.MINUTES.toMillis(1)),
        AUTHONHOUR(TimeUnit.HOURS.toMillis(1)),
        AUTHONHALFDAY(TimeUnit.HOURS.toMillis(12)),
        AUTHONDAY(TimeUnit.HOURS.toMillis(24));

        private final long interval;
        private AuthTimer(long interval) {
            this.interval = interval;
        }

        void start() {
            new Timer().scheduleAtFixedRate(
                    new TimerTask() {
                        @Override
                        public void run() {
                            HBaseUtil.auth(conf, principal, keytab);
                        }
                    },
                    1000,
                    interval
            );

        }
    }


}
