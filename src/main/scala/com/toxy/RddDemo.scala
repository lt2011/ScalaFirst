package com.toxy
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by liutao on 2017/11/28.
 */
class RddDemo {

  def wordCount1() = {
    //    System.setProperty("hadoop.home.dir", "D:\app\spark\hadoop-common-2.2.0-bin-master");
    /**
     * 第一步：
     * 创建sparkConf的配置对象SparkConf,设置Spark程序的运行的配置信息，
     * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL，如果设置为
     * local，则代表Spark在本地运行，特别适合于机器配置条件非常差（例如只有1G的内存的初学者）
     */
    val conf = new SparkConf(); //创建SparkConf对象
    conf.setAppName("wow,My First Spark App!"); //设置应用程序的名称，在spark程序运行的监控界面可以看到名称
    conf.setMaster("local"); //此时，程序在本地运行，不需要安装spark集群

    /**
     * 第二步：
     * 创建SparkContext对象
     * SparkContext是Spark所有程序的唯一入口，无论是使用scala，java，python，R语言等都必须有一个SparkContext（如果具体语言的，类名称不同）
     * SparkContext核心作用是初始化sark应用程序所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
     * 同时还会负责Spark程序往Master注册程序等
     * SparkContext是整个spark应用程序中最为至关重要的一个对象
     */
    val sc = new SparkContext(conf);

    /**
     * 第三步：
     * 根据具体的数据来源（HDFS、Hbase、Local、FS、DB、S3等）通过JavaSparkContext来创建RDD
     * RDD的创建基本有三种方式：根据外部的数据来源（如HDFS）、根据Scala集合、由其他的RDD操作
     * 数据会被RDD划分成为一系列的Partitions，分配到每一个partition的数据属于一个Task的处理范围
     */
    // val lines: RDD[String]=sc.textFile("E://spark-1.6.0-bin-hadoop2.6//spark-1.6.0-bin-hadoop2.6//README.md", 1);//path:文件路径    minPartitions：最小并行度的数量
    val lines = sc.textFile("D:\\Spark\\test.txt", 1); //path:文件路径    minPartitions：最小并行度的数量

    /**
     * 第四步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数的编程，来进行具体的数据计算
     *    4.1步：将每一行的字符串差分成单个单词
     */
    val words = lines.flatMap { lines => lines.split(" ") }; // 对每一行的字符串进行单词拆分，并把所有行的拆分结果通过flat合并成为一个大集合
    print(words)

    /**
     * 第四步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数的编程，来进行具体的数据计算
     *    4.2步：在单词拆分的基础之上对每个单词实例计数为1，也就是word => (word,1)
     */
    val paris = words.map { word => (word, 1) }

    /**
     * 第四步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数的编程，来进行具体的数据计算
     *    4.3步：在每个单词实例计数为1的基础上，统计每个单词在文件中出现的总次数
     */
    val wordCounts = paris.reduceByKey(_ + _) //对相同的key，进行value的累加（包括local和Reducer级别同时Reduce）

    wordCounts.foreach(wordNumberPari => println(wordNumberPari._1 + ":" + wordNumberPari._2))
    wordCounts.saveAsTextFile("D:\\Spark\\result.txt") //将结果保存到文本中

    sc.stop();
  }

}
