package moe.dare.briareus.yarn.reousrces;

import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.yarn.CommonOpts;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Optional;

enum DefaultResourceFactory implements ResourceFactory {
    INSTANCE;

    private static final int DEFAULT_CORES = 1;
    private static final int MINIMUM_CONTAINER_MEM_MB = 128;
    private static final int MEMORY_STEP = 64;

    @Override
    public Resource resources(RemoteJvmOptions options, Resource maximumCapability) {
        int vCores = options.getOpt(CommonOpts.YARN_CONTAINER_CORES).orElse(DEFAULT_CORES);
        Optional<Long> memoryOpt = options.getOpt(CommonOpts.YARN_CONTAINER_MEMORY_MB);
        long containerMem;
        if (memoryOpt.isPresent()) {
            containerMem = memoryOpt.get();
        } else {
            long heapSizeMb = options.maxHeapSize().orElse(0) / 1024 / 1024;
            long overhead = heapSizeMb / 8;
            containerMem = Math.max(MINIMUM_CONTAINER_MEM_MB, heapSizeMb + overhead);
        }
        long delta = containerMem % MEMORY_STEP;
        long steppedMem = delta == 0? containerMem : Math.addExact(containerMem, MEMORY_STEP - delta);
        if (steppedMem > maximumCapability.getMemorySize()) {
            throw new IllegalArgumentException("Unsatisfiable memory resource: required " + steppedMem + " mb of RAM." +
                    "Maximum cluster capability: " + maximumCapability.getMemorySize());
        }
        return Resource.newInstance(steppedMem, vCores);
    }
}
