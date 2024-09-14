package de.samuel.hadoopmetrics.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;

@Configuration
public class HadoopConfig {

    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", "hdfs://127.0.0.1:8020");
        return FileSystem.get(URI.create("hdfs://127.0.0.1:8020"), configuration);
    }
    
}
