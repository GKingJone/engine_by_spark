package com.yisa.engine.test

import org.apache.commons.cli._

object argsTest {
  def main(args: Array[String]): Unit = {
    var cmd: CommandLine = null
    val options = new Options()
    try {
      val zk = new Option("zookeeper", true, "zookeeper host and port.Example 10.0.0.1:2181")
      zk.setRequired(true);
      options.addOption(zk);

      val kafkaConsumerID = new Option("kafkaConsumer", true, "kafka Consumer groupid,recommend non-repeated mark ");
      kafkaConsumerID.setRequired(true);
      options.addOption(kafkaConsumerID);

      val isTest = new Option("isTest", true, "This Program is test or not,1 means beta  version,other means official versionm,if set it 1 ,please stop the program after test");
      isTest.setRequired(true);
      options.addOption(isTest);

      val parser = new PosixParser();
      cmd = parser.parse(options, args);
    } catch {
      case e: Exception => {
        println("Submit SparkEngine Error!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        val formatter = new HelpFormatter()
        formatter.printHelp("This program args list:", options);
        System.exit(1)
      }
    }
    val zkHostPort = cmd.getOptionValue("zookeeper");
    val kafkagroupid = cmd.getOptionValue("kafkaConsumer");
    val test = cmd.getOptionValue("isTest");
    println(zkHostPort)
    println(kafkagroupid)
    println(test)
  }
}