package org.levintech.sparkjobtrigger.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;
import org.levintech.sparkjobtrigger.configuration.properties.SparkJobTriggerProperties;

import javax.servlet.MultipartConfigElement;

/**
 * @author levin
 * Created on 2021/7/30
 */
@Configuration
@EnableConfigurationProperties(
        SparkJobTriggerProperties.class
)
public class CommonConfiguration {
    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        factory.setMaxFileSize(DataSize.of(100, DataUnit.MEGABYTES));
        factory.setMaxRequestSize(DataSize.of(100, DataUnit.MEGABYTES));
        return factory.createMultipartConfig();

    }

}
