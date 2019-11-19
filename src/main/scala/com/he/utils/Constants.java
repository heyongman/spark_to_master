package com.he.utils;

public interface Constants {
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";

    String SPARK_LOCAL = "spark.local";
    String SPARK_LOCAL_TASKID_MONITOR = "spark.local.taskId.monitorFlow";
    String SPARK_LOCAL_TASKID_EXTRACT_CAR= "spark.local.taskId.extractCar";
    String SPARK_LOCAL_WITH_THE_CAR = "spark.local.taskId.withTheCar";
    String SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW = "spark.local.taskid.tpn.road.flow";
    String SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT = "spark.local.taskid.road.one.step.convert";
    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String KAFKA_TOPICS = "kafka.topics";
}
