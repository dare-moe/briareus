package moe.dare.briareus.yarn.sensei;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.api.AMRMClient;

/**
 * An interface for creating Yarn AMRMClient
 */
public interface AMRMClientFactory {
    AMRMClient<AMRMClient.ContainerRequest> armrClient(UserGroupInformation user);
}
