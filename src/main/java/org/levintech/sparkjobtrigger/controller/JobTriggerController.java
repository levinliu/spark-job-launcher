package org.levintech.sparkjobtrigger.controller;

import io.swagger.annotations.ApiOperation;
import org.levintech.sparkjobtrigger.service.JobTriggerHandlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * @author levin
 * Created on 2021/7/29
 */
@RestController("/api/jobTrigger")
public class JobTriggerController {
    private static final Logger log = LoggerFactory.getLogger(JobTriggerController.class);
    @Autowired
    private JobTriggerHandlerService handlerService;

    @ApiOperation("Trigger a spark job submit")
    @GetMapping("/")
    public Map<String, Object> trigger() {
        return handlerService.submitJar();
    }

    @ApiOperation("Upload your spark jar to be executed")
    @PostMapping("/uploadJar")
    public Map<String, String> uploadJar(
            @RequestParam(value = "resource") MultipartFile resource
    ) {
        if (resource == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No resource provided!");
        }
        log.info("receive resource");
        return handlerService.processSparkJar(resource);
    }
}
