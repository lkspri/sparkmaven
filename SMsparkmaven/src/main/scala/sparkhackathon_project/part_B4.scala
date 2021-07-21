package sparkhackathon_project
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object part_B4 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("part_B4").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    // Use RDD functions:
    // 31. Load the file
    val custstates = sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    //custstates.foreach(println)
    
    // 32. Split the above data into 2 RDDs
    val custsplit = custstates.map(x => x.split(","))
    val custfilter = custsplit.filter(x => (x.length == 5))
    val custmap = custfilter.map(x => (x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    //custmap.foreach(println)
    println("===================")
    val statesfilter = custsplit.filter(x => (x.length == 2))
    val statemap = statesfilter.map(x => (x(0),x(1)))
    //statemap.foreach(println)
    
    // Use DSL functions:
    
    // 33. Load the file
    import spark.implicits._
    
    val custstatesdf = spark.read.format("csv")
    .option("delimiter",",")
    .option("inferSchema",true) 
    .option("header",true) 
    .load("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    
    // 34. Split the above data into 2 DFs
    
    val custfilterdf = custmap.toDF("custid","fname","lname","age","prof")
    //custfilterdf.show()
    //custfilterdf.printSchema()
    
    val statesfilterdf = statemap.toDF("statecode","description")
    //statesfilterdf.show()
    //statesfilterdf.printSchema()
    
    custfilterdf.filter("prof is not null").show(100,false)
    custfilterdf.where("prof is null").show(100,false)
    custfilterdf.na.drop().show()
    println("=============")
    statesfilterdf.filter("statecode is not null").show(100,false)
    statesfilterdf.where("statecode is null").show(100,false)
    statesfilterdf.na.drop().show()
    
    custfilterdf.createOrReplaceTempView("custview")
    statesfilterdf.createOrReplaceTempView("statesview")
    
    // 38. Write an SQL query
    //dfinmerge.createOrReplaceTempView("insureview")
    
    // a. Pass NetworkName
    val NN = spark.sql("""select IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,remspecialcharudf(NetworkName) 
      as cleannetworkname,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan from insureview""")
      
    // b. Add current date, current timestamp
    val DT = NN.withColumn("curdt",current_date()).withColumn("curts",current_timestamp())
    
    // c. Extract the year and month from the businessdate
    //spark.sql("select IssuerId,IssuerId2,convert(varchar(10),convert(BusinessDate,getdate(),23),23),StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan from insureview").show()
    
    // d. Extract from the protocol 
    //val pro = spark.sql("""select case when substring(NetworkURL,1,5) = 'http:' then 'http non secure' when substring 
      //(NetworkURL,1,5) = 'https' then 'http secure' else 'noprotocol' end as protocol from insureview""")
     val pro = DT.select(col("*"), (when(col("NetworkURL").startsWith("http:")=== true,"http non secured")
      .when(col("NetworkURL").startsWith("https:") === true,"http secured"))
      .otherwise("noprotocol").alias("protocol"))
      
    // e. Display all the columns  
    /*val joinfile = spark.sql("""select i.cleannetworkname,i.curdt,i.curts,i.protocol,c.age,c.prof,s.statecode from statesview s 
        inner join insureview i on (s.statecode = i.StateCode) 
        join custview c on i.custnum=c.custid""")*/
        
        /*val joinfile = spark.sql("""select i.cleannetworkname,i.curdt,i.curts,i.protocol,c.age,c.prof,s.statecode from statesview s 
        inner join insureview i on (s.statecode = i.StateCode) 
        join custview c on i.custnum=c.custid""")*/
    
    //val df1 = spark.sql("""select t.txnid,c.custid,c.age,c.profession,t.city,t.state from txns t 
                           //inner join custs c on t.custid=c.custid""")
        
    
  }
  
}