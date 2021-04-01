package moe.dare.briareus.yarn.sensei;

import moe.dare.briareus.api.BriareusContext;
import moe.dare.briareus.api.RemoteJvmProcess;

/**
 * Yarn specific BriareusContext for starting containers.
 *
 * @see BriareusYarnSenseiContextBuilder
 */
public interface BriareusYarnSenseiContext extends BriareusContext<RemoteJvmProcess> {
    /**
     * @param status final application status
     */
    void setFinalStatus(ApplicationStatus status);

    /**
     * @param progress progress of application in range [0, 1].
     */
    void setProgress(double progress);
}
