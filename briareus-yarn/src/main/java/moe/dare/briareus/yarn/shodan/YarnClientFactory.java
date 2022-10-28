package moe.dare.briareus.yarn.shodan;

import org.apache.hadoop.yarn.client.api.YarnClient;

/**
 * An interface for creating YarnClient
 */
public interface YarnClientFactory {
    YarnClient yarnClient();
}
