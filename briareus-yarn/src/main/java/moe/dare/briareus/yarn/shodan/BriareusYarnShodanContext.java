package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.api.BriareusContext;

/**
 * Shodan context for submitting new applications to YARN.
 *
 * @see BriareusYarnShodanContextBuilder
 * @see ShodanOpts
 */
public interface BriareusYarnShodanContext extends BriareusContext<YarnSenseiJvmProcess> {
}
