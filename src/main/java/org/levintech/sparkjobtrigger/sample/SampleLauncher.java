package org.levintech.sparkjobtrigger.sample;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * @author levin
 * Created on 2021/7/26
 */
public class SampleLauncher {
    public static void main(String[] args) throws IOException {
        String jarPath = "/Users/mac/Downloads/erkernel/target/er-kernel-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String javaHome = "/usr/java/default/";
        String sparkHome = "/usr/local/spark";
        String master = "local[1]";
        SparkAppHandle handler = new SparkLauncher()
                .setAppName("sparkLaunch")
                .setJavaHome(javaHome)
                .setSparkHome(sparkHome)
                .setMaster(master)
                .setDeployMode("client")
                .setConf("spark.driver.memory", "1g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.instances", "3")
                .setConf(SparkLauncher.EXECUTOR_CORES, "1")
                .setConf(SparkLauncher.CHILD_PROCESS_LOGGER_NAME, "SampleLauncher")
                .setAppResource(jarPath)
                .setMainClass("org.wumiguo.erkernel.Demo")
                .addAppArgs(new String[]{"test"})
                .setVerbose(true)
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        System.out.println("**********  state  changed  **********");
                        System.out.println(handle.getState());
                        if ("Failed".equalsIgnoreCase(handle.getState().toString())) {
                            System.out.println("application error！");
                        }
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                        System.out.println("**********  info  changed  **********");
                        System.out.println(handle.getState());
                    }
                });
        System.out.println("id    " + handler.getAppId());
        System.out.println("state " + handler.getState());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        handler.stop();

    }

}
