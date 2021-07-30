package org.levintech.sparkjobtrigger.component;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author levin
 * Created on 2021/7/26
 */
@Slf4j
@Component
public class SparkLocalJobLauncher {
    private static final Logger log = LoggerFactory.getLogger(SparkLocalJobLauncher.class);

    public Map<String, Object> trigger(
            SparkJobRequest request
    ) throws IOException {
        log.info("will trigger spark-submit {}", request);
        Map<String, Object> status = new HashMap<>();
        SparkAppHandle handler = new SparkLauncher()
                .setAppName("sparkLaunch")
                .setJavaHome(request.javaHome)
                .setSparkHome(request.sparkHome)
                .setMaster(StringUtils.isBlank(request.master) ? "local[1]" : request.master)
                .setDeployMode("client")
                .setConf("spark.driver.memory", "1g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.instances", "3")
                .setConf(SparkLauncher.EXECUTOR_CORES, "1")
                .setConf(SparkLauncher.CHILD_PROCESS_LOGGER_NAME, "SampleLauncher")
                .setAppResource(request.jarPath)
                .setMainClass(request.mainClass)
                .addAppArgs(new String[]{"test"})
                .setVerbose(true)
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        log.info("**********  state  changed  **********");
                        log.info("job state:{}", handle.getState());
                        if ("Failed".equalsIgnoreCase(handle.getState().toString())) {
                            log.info("application error！");
                            status.put("stateChanged", "application error！");
                        }
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                        log.info("**********  info  changed  **********");
                        log.info("job state:{}", handle.getState());
                        status.put("infoChanged", handle.getState());
                    }
                });
        status.put("id", handler.getAppId());
        status.put("state", handler.getState());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        handler.stop();
        return status;
    }

    @ToString
    @Builder(toBuilder = true)
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class SparkJobRequest {

        public String mainClass;
        private String jarPath;
        private String javaHome;
        private String sparkHome;
        private String master;

        @Override
        public String toString() {
            return "SparkJobRequest{" +
                    "mainClass='" + mainClass + '\'' +
                    ", jarPath='" + jarPath + '\'' +
                    ", javaHome='" + javaHome + '\'' +
                    ", sparkHome='" + sparkHome + '\'' +
                    ", master='" + master + '\'' +
                    '}';
        }

        public String getMainClass() {
            return mainClass;
        }

        public void setMainClass(String mainClass) {
            this.mainClass = mainClass;
        }

        public String getJarPath() {
            return jarPath;
        }

        public void setJarPath(String jarPath) {
            this.jarPath = jarPath;
        }

        public String getJavaHome() {
            return javaHome;
        }

        public void setJavaHome(String javaHome) {
            this.javaHome = javaHome;
        }

        public String getSparkHome() {
            return sparkHome;
        }

        public void setSparkHome(String sparkHome) {
            this.sparkHome = sparkHome;
        }

        public String getMaster() {
            return master;
        }

        public void setMaster(String master) {
            this.master = master;
        }
    }

}
