package org.levintech.sparkjobtrigger.service;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.levintech.sparkjobtrigger.component.FileLocalCopier;
import org.levintech.sparkjobtrigger.component.SparkLocalJobLauncher;
import org.levintech.sparkjobtrigger.configuration.properties.SparkJobTriggerProperties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * @author levin
 * Created on 2021/7/27
 */
@Service
public class JobTriggerHandlerService {
    private static final Logger log = LoggerFactory.getLogger(JobTriggerHandlerService.class);

    @Autowired
    private FileLocalCopier copier;
    @Autowired
    private SparkLocalJobLauncher jobLauncher;
    @Autowired
    private SparkJobTriggerProperties jobTriggerProperties;

    public Map<String, String> processSparkJar(MultipartFile resource) {
        InputStream in;
        try {
            in = resource.getInputStream();
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Fail to get the uploaded file data", e);
        }
        log.info("about to save jar to path {}", jobTriggerProperties.getSparkJarPath());
        String curDir = System.getProperty("user.dir");
        Path folder = Paths.get(curDir + "/" + jobTriggerProperties.getSparkJarPath()).getParent();
        try {
            FileUtils.forceMkdir(folder.toFile());
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Fail to create local jar dir", e);
        }
        copier.save(in, curDir + "/" + jobTriggerProperties.getSparkJarPath());
        Map<String, String> status = new HashMap<>();
        status.put("msg", jobTriggerProperties.getSparkJarPath());
        return status;
    }

    public Map<String, Object> submitJar() {
        SparkLocalJobLauncher.SparkJobRequest req = new SparkLocalJobLauncher.SparkJobRequest();
        req.setJarPath(jobTriggerProperties.getSparkJarPath());
        req.setJavaHome(jobTriggerProperties.getJavaHome());
        req.setSparkHome(jobTriggerProperties.getSparkHome());
        req.setMaster(jobTriggerProperties.getMaster());
        req.setMainClass(jobTriggerProperties.getMainClass());
        try {
            return jobLauncher.trigger(req);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Fail to submit spark job", e);
        }
    }
}
