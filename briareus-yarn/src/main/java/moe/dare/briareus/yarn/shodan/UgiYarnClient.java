package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.JvmStartFailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;

import java.security.PrivilegedAction;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

class UgiYarnClient {
    private final YarnClient client = YarnClient.createYarnClient();
    private final Supplier<UserGroupInformation> ugi;

    UgiYarnClient(Supplier<UserGroupInformation> ugi) {
        this.ugi = requireNonNull(ugi, "ugi");
    }

    void start(Configuration configuration) {
        ugi.get().doAs((PrivilegedAction<Void>)() -> {
            client.init(configuration);
            client.start();
            return null;
        });
    }

    void stop() {
        ugi.get().doAs((PrivilegedAction<Void>)() -> {
            client.stop();
            return null;
        });
    }

    YarnClientApplication createApplication() {
        return ugi.get().doAs((PrivilegedAction<YarnClientApplication>)() -> {
            try {
                return client.createApplication();
            } catch (Exception e) {
                throw new JvmStartFailedException("Can't create yarn application", e);
            }
        });
    }

    ApplicationId submitApplication(ApplicationSubmissionContext context) {
        return ugi.get().doAs((PrivilegedAction<ApplicationId>)() -> {
            try {
                return client.submitApplication(context);
            } catch (Exception e) {
                throw new JvmStartFailedException("Can't submit yarn application", e);
            }
        });
    }

    ApplicationReport getApplicationReport(ApplicationId id) {
        return ugi.get().doAs((PrivilegedAction<ApplicationReport>)() -> {
            try {
                return client.getApplicationReport(id);
            } catch (Exception e) {
                throw new BriareusException("Can't get application report", e);
            }
        });
    }

    boolean isStopped() {
        return client.isInState(Service.STATE.STOPPED);
    }

    void killApplication(ApplicationId applicationId) {
        requireNonNull(applicationId, "applicationId");
        ugi.get().doAs((PrivilegedAction<Void>)() -> {
            try {
                client.killApplication(applicationId);
                return null;
            } catch (Exception e) {
                throw new BriareusException("Can't kill application", e);
            }
        });
    }
}
