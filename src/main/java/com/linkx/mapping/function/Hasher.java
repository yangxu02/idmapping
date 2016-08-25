package com.linkx.mapping.function;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public enum Hasher {
    md5 {
        @Override
        public String hash(String input) {
            return Hashing.md5().hashString(input, Charsets.UTF_8).toString();
        }
    },

    sha1 {
        @Override
        public String hash(String input) {
            return Hashing.sha1().hashString(input, Charsets.UTF_8).toString();
        }
    },

    sha256 {
        @Override
        public String hash(String input) {
            return Hashing.sha256().hashString(input, Charsets.UTF_8).toString();
        }
    },

    sha512 {
        @Override
        public String hash(String input) {
            return Hashing.sha512().hashString(input, Charsets.UTF_8).toString();
        }
    },

    /*
    crc32 {
        @Override
        public String hash(String input) {
            return Hashing.crc32().hashString(input, Charsets.UTF_8).toString();
        }
    },
    */

    murmur3_128 {
        @Override
        public String hash(String input) {
            return Hashing.murmur3_128().hashString(input, Charsets.UTF_8).toString();
        }
    },

    murmur3_32 {
        @Override
        public String hash(String input) {
            return Hashing.murmur3_32().hashString(input, Charsets.UTF_8).toString();
        }
    },

    plain {
        @Override
        public String hash(String input) {
            return input;
        }
    };

    public abstract String hash(String input);

}
