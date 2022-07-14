package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.yarn.launch.files.UploadedEntry;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

class FsKey {
    private final String scheme;
    private final String userInfo;
    private final String host;
    private final int port;

    static FsKey keyFor(UploadedEntry entry) {
        return new FsKey(entry.resource().getResource());
    }

    private FsKey(org.apache.hadoop.yarn.api.records.URL resource) {
        this.scheme = resource.getScheme();
        this.userInfo = resource.getUserInfo();
        this.host = resource.getHost();
        this.port = resource.getPort();
    }

    URI toFsUri() throws URISyntaxException {
        return new URI(scheme, userInfo, host, port, null, null, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FsKey that = (FsKey) o;
        return port == that.port &&
                Objects.equals(scheme, that.scheme) &&
                Objects.equals(userInfo, that.userInfo) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheme, userInfo, host, port);
    }

    @Override
    public String toString() {
        return "Filesystem Key {" +
                "scheme='" + scheme + '\'' +
                ", userInfo='" + (userInfo == null ? "<null>" : "<*****>") + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
