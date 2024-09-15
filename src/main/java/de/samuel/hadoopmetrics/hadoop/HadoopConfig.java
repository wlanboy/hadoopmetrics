package de.samuel.hadoopmetrics.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;

@Configuration
public class HadoopConfig {

    @Value("${hadoop.defaultFS}")
    private String defaultFS;
    @Value("${hadoop.directoryPath}")
    public String directoryPath;
    
    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", defaultFS);
        return FileSystem.get(URI.create(defaultFS), configuration);
    }
    
}
