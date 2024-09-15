package de.samuel.hadoopmetrics.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;

@Service
public class HadoopService {
    
    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private HadoopConfig config;

    @Autowired
    private MeterRegistry meterRegistry;

    private Map<String, Long> directorySizes = new HashMap<>();

    @Scheduled(fixedRate = 3600000) //every hour
    private void updateMetrics() throws IOException, URISyntaxException {
        updateDirectorySizes();
        generatePrometheusMetrics();
    }

    public void updateDirectorySizes() throws IOException, URISyntaxException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(config.directoryPath));
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                URI uri = new URI(status.getPath().toString());
                Long size = fileSystem.getContentSummary(status.getPath()).getLength();
                directorySizes.put(uri.getPath(), size);
            }
        }
    }

    public void generatePrometheusMetrics() {
        directorySizes.forEach((path, size) -> 
            meterRegistry.gauge("hadoop.directory.size." + path, size)
        );
    }

    public Map<String, Long> getDirectorySizes() {
        return directorySizes;
    }
}
