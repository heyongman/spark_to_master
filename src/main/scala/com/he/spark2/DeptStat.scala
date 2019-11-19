package com.he.spark2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * 统计年龄20岁以上的员工
  * 按部门名称和员工性别为粒度统计
  * 统计出每个部门分性别的平均薪资和年龄
  */
object DeptStat {

  case class Dept(id: String, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2Demo")
      .master("local[4]")
      //      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val dept = spark.read.json("/Users/he/proj/bigdata_proj/spark_to_master/src/main/resources/department.json")
    val employee = spark.read.json("/Users/he/proj/bigdata_proj/spark_to_master/src/main/resources/employee.json")
    val employee2 = spark.read.json("/Users/he/proj/bigdata_proj/spark_to_master/src/main/resources/employee2.json")


    /**
      * typed操作
      */
    //    算子方式
    val df = employee
      .filter($"age" > 20)
      .join(dept, $"id" === $"depId")
      .groupBy(dept("name"), employee("gender"))
      .agg(avg(employee("salary")), avg(employee("age")))
    //    df.show()

    //    sql方式
    dept.createOrReplaceTempView("dept")
    employee.createOrReplaceTempView("employee")
    //    spark.sql("select b.name,a.gender,avg(a.salary) as avg_salary,avg(a.age) as avg_age from employee a left join dept b on a.depId = b.id  where a.age > 20 group by b.name,a.gender").explain()
    //    spark.sql("select b.name,a.gender,avg(a.salary) as avg_salary,avg(a.age) as avg_age from employee a left join dept b on a.depId = b.id  where a.age > 20 group by b.name,a.gender").show()


    val deptDS: Dataset[Dept] = dept.as[Dept]

    //    两种去重方式
    employee.distinct()
    employee.dropDuplicates("name")

    //    获取交集
    employee.intersect(employee2)
    //    差集 在a中有但是不在b中的
    employee.except(employee2)
    //    并集
    employee.union(employee2)
    //    连接
    employee.join(dept, employee("depId") === dept("id"))
    employee.joinWith(dept, $"depId" === $"id")
    //    排序
    employee.sort($"salary".desc)
    //    数据切分 抽取
    employee.randomSplit(Array(2, 3, 3)).foreach(x => x)
    employee.sample(false, 0.5)

//    full join
    val ints1 = Seq(("a","2018",1),("b","2019",2),("c","2017",3)).toDF("name","date","value1")
    val ints2 = Seq(("a","2018",2),("a","2017",1),("d","2016",4)).toDF("name","date","value2")
    ints1
      .join(ints2,ints1("name") <=> ints2("name") && ints1("date") <=> ints2("date"),"full")
      .select(
        coalesce(ints1("name"),ints2("name")).alias("coalesce_name"), //合并相同的字段
        coalesce(ints1("date"),ints2("date")).alias("coalesce_date"),
        $"value1",
        $"value2"
      )
      .na.fill(0)
      .show()

    /**
      * untyped操作
      */
    employee
      .where($"age" > 20)
      .where($"name".contains("Tom"))
      .join(dept, dept("id") === employee("depId"))
      .groupBy(dept("name"), employee("gender"))
      .agg(avg(employee("salary")))
      .select("name", "gender", "avg(salary)")

//    列转行
    employee
        .groupBy(employee("depId"))
        .agg(collect_list(employee("name")),collect_set(employee("name")))
        .show(false)



    spark.stop()
  }
}


