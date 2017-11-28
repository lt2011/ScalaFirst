package com.toxy

import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liutao on 2017/11/28.
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "D:\\app\\spark\\hadoop-common-2.2.0-bin-master"); //设置hadoop的位置，不设置环境变量
//    method2()
    val rddDemo = new RddDemo
    rddDemo.wordCount1()

  }

  def defaultMethod() = {
    println("Hello World")
    val str = "hello" //val定义的变量不能被重新赋值，var的才可以。val相当于final
    println(add(3, 2));
    val helloWorldDoubleQuote = "\"Hello World\""
    //如果希望能够原样输出字符串中的内容，则用三个引号"""将字符串包裹起来
    println( """ hello cruel world, \n \\\\ \b \\, I am " experienced" programmer""")
    println(helloWorldDoubleQuote);

    val floatNumber = 0.314529e2 //乘以10的二次方
    var sumVlaue = 1 + 2
    var ty = 1 + -3 //相当于1加负3，scala中可以用+ -符号来表示正负数，例如-3 +3，并且可以加入到运算符当中
    println(floatNumber)

    //Scala中的对象比较不同于Java中的对象比较
    //Scala基于内容比较，而java中比较的是引用，进行内容比较时须定义比较方法
    val x = "hi"
    val y = "hi"
    println(x == y) //打印true
    println(gcdLoop(4, 3))

    for (i <- 1 to 5) println("Iteration" + i)
  }

  def add(a: Int, b: Int): Int = {
    a + b
  }

  //if使用
  def ifMethod() = {
    val x = if ("hello" == "hell") 1 else 0
  }

  //while结构
  def gcdLoop(x: Long, y: Long): Long = {
    var a = x
    var b = y
    while (a != 0) {
      val temp = a
      a = b % a
      b = temp
    }
    b
  }

  def forMethod() = {
    val filesHere = (new java.io.File(".")).listFiles
    for (file <- filesHere if file.getName.endsWith(".scala"))
      println(file)
    //还可以加入多个过滤条件，用;隔开
    for (
      file <- filesHere if file.isFile
      if file.getName.endsWith(".scala")
    ) println(file)
  }

  //do-while结构
  //  var line = ""
  //  do {
  //    line = readLine()
  //    println("Read: " + line)
  //  } while (line != "")


  def method1() = {
    val conf = new SparkConf(); //创建SparkConf对象
    conf.setAppName("MySparkAPP!"); //设置应用程序的名称
    conf.setMaster("local")
    //此时，程序在本地运行，不需要安装spark集群
    val sc = new SparkContext(conf)
    //创建SparkContext对象
    val lines = sc.textFile("D:\\log.txt", 2)
    //      lines.foreach(println)
    lines.count()
    println("***********************")
    val line2 = lines.filter(line => line.contains("jdbc"))
    line2.foreach(println)

    println("Map后的结果为")
    val rdd_Map = lines.map(line => line.split(" ")) //map将每行内容作为一个对象，打印出来的是每行对象
    rdd_Map.foreach(println)

    println("flatMap后的结果为")
    val rdd_flatMap = lines.flatMap(line => line.split(" ")) //做完上一步Map后，再flatMap将内容压扁，每个单词成为一个内容，可以打印输出
    rdd_flatMap.foreach(println)

  }


  def method2() = {
    val conf = new SparkConf(); //创建SparkConf对象
    conf.setAppName("MySparkAPP!"); //设置应用程序的名称
    conf.setMaster("local"); //此时，程序在本地运行，不需要安装spark集群
    val sc = new SparkContext(conf); //创建SparkContext对象
    val rdd = sc.parallelize(Array(1, 2, 2, 4))
    println(rdd.count())
    rdd.foreach(println) //打印出rdd的内容，测试时使用
    println(rdd.collect()) //collect是遍历整个rdd，测试时使用，大数据时要saveAs保存起来到文件中

    println("*************reduce后的结果为**********")
    val res1 = rdd.reduce((x, y) => x * y) //reduce将RDD中元素前两个传给输入函数，产生一个新的return值，新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，再被传给输入函数，直到最后只有一个值为止
    println(res1)
    println("*************take后的结果为**********")
    val res2 = rdd.take(3) //随机取三个元素，返回一个Array对象
    println(res2.foreach(println))
    println("*************top后的结果为**********")
    val res3 = rdd.top(2) //排序后取出两个元素，返回一个Array对象
    println(res3.foreach(println))
  }


  //RDD的集合运算,去重，联合，求交，求差
  def mehtod3() = {
    val conf = new SparkConf(); //创建SparkConf对象
    conf.setAppName("MySparkAPP!"); //设置应用程序的名称
    conf.setMaster("local"); //此时，程序在本地运行，不需要安装spark集群
    val sc = new SparkContext(conf); //创建SparkContext对象

    val rdd1 = sc.parallelize(Array("coffee", "coffee", "monkey", "tea", "panda"))
    val rdd1_result1 = rdd1.distinct() //去除重复元素
    rdd1_result1.foreach(println)

    val rdd2 = sc.parallelize(Array("coffee", "coffee", "monkey"))
    val rdd_result2 = rdd1.union(rdd2) //两个rdd的联合
    rdd_result2.foreach(println)

    val rdd_result3 = rdd1.intersection(rdd2)
    rdd_result3.foreach(println) //求两个rdd的交集

    val rdd_result4 = rdd1.subtract(rdd2) //两个rdd的差集，rdd1中有的，rdd2中没有
    println("rdd1-rdd2的结果为")
    rdd_result4.foreach(println)

    val rdd_result5 = rdd1.cartesian(rdd2) //两个rdd的笛卡尔积
    println("rdd1-rdd2的结果为")
    rdd_result4.foreach(println)

  }

  //key/value用法
  def method4() = {
    val sc = new SparkContext(new SparkConf().setAppName("LIU").setMaster("local"))
    val rdd = sc.textFile("D:\\Spark\\test.txt", 1)
    rdd.foreach(println)

    val rdd2 = rdd.map(line => (line.split(" ")(0), line))
    rdd2.foreach(println)

    val rdd3 = sc.parallelize(List((1, 2), (1, 3), (3, 4), (3, 6))) //找到key相同的对应的V值，然后进行乘法操作
    val rdd4 = rdd3.reduceByKey((x, y) => x * y) //1相同对应的value值有2和3，则2*3=6
    rdd4.foreach(println)

    val gBKRdd = rdd3.groupByKey() //找到相同的key对应的value值，并进行汇总分组，如key=1能找到对应的value值有2和3，则分组为(1,CompactBuffer(2, 3))
    gBKRdd.foreach(println)

    val mapValuesRdd = rdd3.mapValues(x => x + 1)//key不变，将key对应的value值加1
    mapValuesRdd.foreach(println)

    val flatMapValuesRdd=rdd3.flatMapValues(x=>(x to 5))//针对每个键值对，并且key值小于5的，生成新的多个value值小于5的键值对
    flatMapValuesRdd.foreach(println)

    val keys=rdd3.keys  //返回所有key
    val values=rdd3.values //返回所有valus

    val sortByKeyRdd=rdd3.sortByKey()

  }


}
