package de.samuel.hadoopmetrics.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class HadoopService {
    
    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private MeterRegistry meterRegistry;

    private Map<String, Long> directorySizes = new HashMap<>();

    public void updateDirectorySizes(String directoryPath) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(directoryPath));
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                directorySizes.put(status.getPath().toString(), status.getLen());
            }
        }
        generatePrometheusMetrics();
    }

    private void generatePrometheusMetrics() {
        directorySizes.forEach((path, size) -> 
            meterRegistry.gauge("hadoop.directory.size." + Map.of("path", path), size)
        );
    }

    public Map<String, Long> getDirectorySizes() {
        return directorySizes;
    }
}
