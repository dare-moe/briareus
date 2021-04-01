package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.api.RemoteJvmProcess;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

public interface YarnSenseiJvmProcess extends RemoteJvmProcess {
    FinalApplicationStatus completionStatus();
}
