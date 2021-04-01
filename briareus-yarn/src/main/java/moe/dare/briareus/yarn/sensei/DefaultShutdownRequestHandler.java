package moe.dare.briareus.yarn.sensei;

import moe.dare.briareus.yarn.launch.DefaultLaunchContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum DefaultShutdownRequestHandler implements Runnable {
    INSTANCE;
    private static final Logger log = LoggerFactory.getLogger(DefaultLaunchContextFactory.class);

    @Override
    public void run() {
        log.warn("Ignoring yarn shutdown request");
    }
}
