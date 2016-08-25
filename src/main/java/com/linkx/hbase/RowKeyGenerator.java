package com.linkx.hbase;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.UUID;

/**
 * Created by ulyx.yang@ndpmedia.xom on 10/29/14.
 * hbase row key generator
 */
public enum RowKeyGenerator {
    // random uuid as row key
    uuid {
        public byte[] next() {
            return UUID.randomUUID().toString().getBytes(Charsets.UTF_8);
        }

        @Override
        public byte[] from(String... params) {
            return next();
        }
    },

    // split into n regions by hash
    hash {
        public byte[] next() {
            String rk = UUID.fromString(new Date().getTime() + "").toString();
            return getRowKey(rk, "");
        }

        /**
         * @param params
         * param[0]: key seed
         * param[1]: region number
         * param[2]: key prefix
         * @return
         */
        @Override
        public byte[] from(String... params) {
            if (null != params) {
                if (params.length == 2) {
                    return getRowKey(params[0], params[1]);
                } else if (params.length >= 3) {
                    return getRowKey(params[0], params[1], params[2]);
                }
            }
            return next();
        }

        /**
         *
         * @param params
         * param[0]: key seed
         * param[1]: region number
         * param[2]: key prefix
         * @return
         */
        @Override
        public byte[] reverse(String... params) {
            if (null == params || params.length < 2)
                return new byte[0];
            if (params.length == 2)
                return getOriginRowKey(params[0], params[1]);
            else if (params.length >= 3) {
                return getOriginRowKey(params[0], params[1], params[2]);
            }
            return new byte[0];
        }

        /**
         * get original key from hashed string
         * eg :
         * 31-solo|000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         *     return 000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         * 31-solo|000014ce5791d2da3d1ee5c99fecb59caf1c5dab|abc
         *     return 000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         * @param rk: hashed row key
         * @param nRegions: n buckets
         * @param prefix: prefix string
         * @return
         */
        private byte[] getOriginRowKey(String rk, String nRegions, String prefix) {
            Integer n = Ints.tryParse(nRegions);
            rk = Strings.nullToEmpty(rk);
            if (null == n) {
                return rk.getBytes();
            }

            int s = rk.indexOf('|');
            int e = rk.indexOf('|', s + 1);
            if (e == -1) {
                e = rk.length();
            }
            return Bytes.toBytes(rk.substring(s + 1, e));

        }

        /**
         * get original key from hashed string
         * eg :
         * 28-000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         *     return 000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         * 28-000014ce5791d2da3d1ee5c99fecb59caf1c5dab|abc
         *     return 000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         * @param rk: hashed row key
         * @param nRegions: n buckets
         * @return
         */
        private byte[] getOriginRowKey(String rk, String nRegions) {
            Integer n = Ints.tryParse(nRegions);
            rk = Strings.nullToEmpty(rk);
            if (null == n) {
                return rk.getBytes();
            }

            int s = rk.indexOf('-');
            int e = rk.indexOf('|', s + 1);
            if (e == -1) {
                e = rk.length();
            }
            return Bytes.toBytes(rk.substring(s + 1, e));
        }

        /**
         * generator row with prefix
         * eg:
         * 000014ce5791d2da3d1ee5c99fecb59caf1c5dab, 32, solo
         * return 31-solo|000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         * @param rk: row key seed
         * @param nRegions: n buckets
         * @param vPrefix: prefix String
         * @return
         */
        private byte[] getRowKey(String rk, String nRegions, String vPrefix) {
            Integer n = Ints.tryParse(nRegions);
            rk = Strings.nullToEmpty(rk);
            if (null == n) {
                return rk.getBytes();
            }
            rk = vPrefix + "|" + rk;
            String prefix = Math.abs(rk.hashCode() % n) + "";
            int padWitdh = 1;
            if (n >= 10 && n < 100) {
                padWitdh = 2;
            } else if (n >= 100 && n < 1000) {
                padWitdh = 3;
            } else {
                while (n >= 10) {
                    padWitdh++;
                    n /= 10;
                }
            }
            prefix = Strings.padStart(prefix, padWitdh, '0');

            byte[] rowKey = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes("-"), Bytes.toBytes(rk));
            return rowKey;
        }

        /**
         * generator row with prefix
         * eg:
         * 000014ce5791d2da3d1ee5c99fecb59caf1c5dab, 32
         * return 28-000014ce5791d2da3d1ee5c99fecb59caf1c5dab
         * @param rk: row key seed
         * @param nRegions: n buckets
         * @return
         */
        private byte[] getRowKey(String rk, String nRegions) {
            Integer n = Ints.tryParse(nRegions);
            rk = Strings.nullToEmpty(rk);
            if (null == n) {
                return rk.getBytes();
            }
            String prefix = Math.abs(rk.hashCode() % n) + "";
            int padWitdh = 1;
            if (n >= 10 && n < 100) {
                padWitdh = 2;
            } else if (n >= 100 && n < 1000) {
                padWitdh = 3;
            } else {
                while (n >= 10) {
                    padWitdh++;
                    n /= 10;
                }
            }
            prefix = Strings.padStart(prefix, padWitdh, '0');

            byte[] rowKey = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes("-"), Bytes.toBytes(rk));
            return rowKey;
        }
    },

    // use input as row key
    plain {
        public byte[] next() {
            return UUID.fromString(new Date().getTime() + "").toString().getBytes(Charsets.UTF_8);
        }

        @Override
        public byte[] from(String... params) {
            if (null == params || params.length == 0)
                return next();
            return Bytes.toBytes(params[0]);
        }
    },

    // use current timestamp
    timestamp {
        public byte[] next() {
            return (new Date().getTime() + "").getBytes(Charsets.UTF_8);
        }

        @Override
        public byte[] from(String... params) {
            return next();
        }
    };

    public abstract byte[] next();

    public abstract byte[] from(String... params);

    public byte[] reverse(String... params) {
        if (null == params || params.length == 0)
            return new byte[0];
        if (params.length == 2) {
            int index = params[0].indexOf('|');
            return Bytes.toBytes(params[0].substring(index + 1));
        }
        return Bytes.toBytes(params[0]);
    }

    public final static RowKeyGenerator fromString(String type, String def) {
        try {
            return valueOf(type);
        } catch (Exception e) {

        }
        return valueOf(def);
    }
}
