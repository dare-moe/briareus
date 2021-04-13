package moe.dare.briareus.yarn.testtools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.security.token.Token;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class DelegateTestFs extends FilterFileSystem {
    public static final String SCHEME = "delegatefs";
    private static final String FS_IMPL_KEY = "fs." + SCHEME + ".impl";
    private static final String FS_CACHE_KEY = "fs." + SCHEME + ".impl.disable.cache";
    private static final Map<String, FileSystem> DELEGATES = new ConcurrentHashMap<>();

    public static Closeable registerDelegate(String virtualHost, FileSystem instance) {
        requireNonNull(virtualHost, "virtualHost");
        if (DELEGATES.putIfAbsent(virtualHost, instance) != null) {
            throw new IllegalStateException("Virtual host registered: " + virtualHost);
        }
        return () -> DELEGATES.remove(virtualHost);
    }

    public static Configuration createConfiguration(Configuration base) {
        Configuration conf = new Configuration(base);
        configure(conf);
        return conf;
    }

    public static void configure(Configuration conf) {
        conf.setClass(FS_IMPL_KEY, DelegateTestFs.class, FileSystem.class);
        conf.setBoolean(FS_CACHE_KEY, true);
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        String host = name.getHost();
        fs = DELEGATES.get(host);
        if (fs == null) {
            throw new IOException("No virtual host named: " + host);
        }
    }

    @Override
    public String getCanonicalServiceName() {
        return "delegatefs";
    }

    @Override
    public FileSystem[] getChildFileSystems() {
        return fs.getChildFileSystems();
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        return fs.getDelegationToken(renewer);
    }

    @Override
    public void close() {
    }
}
