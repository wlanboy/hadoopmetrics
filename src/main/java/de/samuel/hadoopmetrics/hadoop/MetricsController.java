package de.samuel.hadoopmetrics.hadoop;

import java.io.IOException;
import java.net.URISyntaxException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricsController {
    @Autowired
    private HadoopService hadoopService;

    @GetMapping("/metricsstart")
    public String metrics() {
        try {
            hadoopService.updateDirectorySizes();
            hadoopService.generatePrometheusMetrics();
        } catch (IOException | URISyntaxException e) {
            return "Metrics error" + e.getMessage();
        }
        return "Metrics updated";
    }
}
