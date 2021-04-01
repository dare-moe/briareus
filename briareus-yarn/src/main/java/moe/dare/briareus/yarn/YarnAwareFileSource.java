package moe.dare.briareus.yarn;

import moe.dare.briareus.api.FileSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

public interface YarnAwareFileSource extends FileSource {
    Path resourcePath();

    LocalResourceVisibility resourceVisibility();
}
