package moe.dare.briareus.local;

import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.FileSource;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.api.RemoteJvmProcess;
import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.*;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

class BriareusLocalContextTest {
    private static final String EXIT_CODE_MAIN = "moe.dare.testjar.ExitCode";
    private static final String HELLO_WORLD_MAIN = "moe.dare.testjar.HelloWorld";

    @TempDir
    Path tempDir;
    private ExecutorService executorService;
    private ProcessMonitor processMonitor;
    private WorkDirectoryFactory workDirectoryFactory;
    private BriareusLocalContext context;
    private final FileSource testJar = () -> BriareusLocalContext.class.getResourceAsStream("/test.jar");

    @BeforeEach
    void setUp() {
        ThreadFactory tf = ThreadFactoryBuilder.withPrefix("test-local-context").deamon(true).build();
        ThreadFactory tfMonitor = ThreadFactoryBuilder.withPrefix("test-local-context-monitor").deamon(true).build();
        executorService = Executors.newSingleThreadExecutor(tf);
        processMonitor = PollingProcessMonitor.create(Duration.ofMillis(100), tfMonitor);
        workDirectoryFactory = DefaultWorkDirectoryFactory.create(tempDir.resolve("jvms"));
        context = new BriareusLocalContext(workDirectoryFactory, processMonitor, executorService);
    }

    @AfterEach
    void tearDown() throws Exception {
        context.close();
        workDirectoryFactory.close();
        processMonitor.close();
        executorService.shutdownNow();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    void testExitCode() throws Exception {
        // given
        RemoteJvmOptions options = RemoteJvmOptions.newBuilder()
                .addFiles(singletonList(FileEntry.copy(testJar, "distributed.jar")))
                .addClasspath(singletonList("distributed.jar"))
                .mainClass(EXIT_CODE_MAIN)
                .addArgument("42")
                .maxHeapSize(1024L * 1024 * 10)
                .build();
        // when
        CompletableFuture<RemoteJvmProcess> future = context.start(options);
        // then
        RemoteJvmProcess process = future.get(1, TimeUnit.SECONDS);
        process.onExit().toCompletableFuture().get(1, TimeUnit.SECONDS);
        assertThat(process.isAlive()).isFalse();
        assertThat(process.exitCode()).hasValue(42);
    }
}