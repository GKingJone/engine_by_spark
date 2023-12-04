一、SparkEngine
1、启动命令
/opt/spark/bin/spark-submit \
--class com.yisa.engine.trunk.SparkEngineVer2 \
--name SparkEngineVer2 \
--jars /moma/mysql-connector-java-5.1.39.jar,/app/spark/ZookeeperUtil-1.0-jar-with-dependencies.jar \
--driver-class-path  /moma/mysql-connector-java-5.1.39.jar:/app/spark/ZookeeperUtil-1.0-jar-with-dependencies.jar \
--master spark://gpu1:6066 \
--deploy-mode cluster \
--supervise   \
--total-executor-cores 50 \
--executor-memory 4G  \
/moma/YISAEngineBySpark2-1.0.1-SNAPSHOT-jar-with-dependencies.jar \
--zookeeper gpu3:2181 \
--kafkaConsumer gid1610170826001 \
--isTest release  
注：
zookeeper：集群zookeeper地址
kafkaConsumer：kafka消费组id,建议使用gid+日期+序号，如gid1601010101010
isTest 正式版本使用release，从kafka SparkEngine读取请求;  测试版本使用debug,从kafka SparkEngineTest读取请求   

2、请求模式
SparkEngine的请求从Kafka中读取，发送请求方式另见SparkEngineUtil项目
3、请求规则
请求规则为发送到Kafka的数据（字符型）格式，见下
task_type|job_id|job_message

task_type
请参照com.yisa.engine.common.CommonTaskType，此类已锁定，如果没有需要任务类型时，请暂勿自行添加，需协调后才能加入


job_id
内容必须包含唯一字符串，确保不会重复，建议使用UUID

job_message
请参照com.yisa.engine.common.InputBean类，内容为此类的Json字符串，此类此类已锁定，没有需要查询条件时，请暂勿自行添加，需协调后才能加入

实例：

01|testjob|{"startTime:":"20161013000000","endTime":"20161014000000","count":"2"}






