package org.levintech.sparkjobtrigger.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author levin
 * Created on 2021/7/29
 */
@ConfigurationProperties("spark-job-trigger")
public class SparkJobTriggerProperties {
    private String sparkJarPath;
    private String sparkHome;
    private String javaHome;
    private String master;
    private String mainClass;

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public void setSparkHome(String sparkHome) {
        this.sparkHome = sparkHome;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    public String getSparkJarPath() {
        return sparkJarPath;
    }

    public void setSparkJarPath(String sparkJarPath) {
        this.sparkJarPath = sparkJarPath;
    }
}
