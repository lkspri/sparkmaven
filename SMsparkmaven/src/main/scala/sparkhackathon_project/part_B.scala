package sparkhackathon_project

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{SparkSession,Row,DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.regexp_replace
import org.inceptez.hack.allmethods

object part_B {
  
  def main(args:Array[String]) : Unit=
  {
    val spark = SparkSession.builder().appName("PartB").master("local")
    //.config("hive.metastore.uris","thrift://localhost:9083")
    //.enableHiveSupport()
    .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions",4)
    
    // 21. Create structuretype
    val insurance = StructType(List(
          StructField("IssuerId",IntegerType,true), 
          StructField("IssuerId2",IntegerType,true), 
          StructField("BusinessDate",StringType,true), 
          StructField("StateCode",StringType,true), 
          StructField("SourceName",StringType,true),
          StructField("NetworkName",StringType,true),
          StructField("NetworkURL",StringType,true),
          StructField("custnum",StringType,true),
          StructField("MarketCoverage",StringType,true),
          StructField("DentalOnlyPlan",StringType,true)))
    // 22. Create dataframes 
    //import spark.implicits._     
    val dfin1 = spark.read.format("csv")
            .schema(insurance)
            .option("delimiter",",")
            .option("mode","DROPMALFORMED")
            .option("header",true)
            .option("inferSchema",true)
            .load("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv")
            //dfin1.show()
            //dfin1.count()
       
        
    val dfin2 = spark
            .read
            .format("csv")
            .schema(insurance)
            .option("delimiter",",")
            .option("mode","DROPMALFORMED")
            .option("header",true)
            .option("dateFormat","yyy-mm-dd")
            .option("inferSchema",true)
            .load("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
            //dfin2.show()
            
// 23. Apply the below DSL functions
            
    // a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.
    val dfrename = dfin1.select(col("IssuerId"),col("IssuerId2"),col("BusinessDate"),col("StateCode").alias("stcd")
    ,col("DentalOnlyPlan"))
    val dfrename1 = dfin2.select(col("IssuerId"),col("IssuerId2"),col("BusinessDate"),col("StateCode").alias("stcd")
        ,col("SourceName").alias("srcnm"),col("NetworkName"),col("NetworkURL"),col("custnum"),col("MarketCoverage")
        ,col("DentalOnlyPlan"))
        
    // b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field 
    val dfconcat =dfin1.select(concat(col("IssuerId"),lit(" "),col("IssuerId2")).as("issueridcomposite"))
    val dfconcat1 =dfin2.select(concat(col("IssuerId"),lit(" "),col("IssuerId2")).as("issueridcomposite"))

    // c. Remove DentalOnlyPlan column
    val dfdrop = dfin1.drop("DentalOnlyPlan")
    val dfdrop1 = dfin2.drop("DentalOnlyPlan")

    // d. Add columns that should show the current system date and timestamp with the 
    val dfcurdatets = dfin1.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())
    val dfcurdatets1 = dfin2.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())
        
    // i,ii,iii,iv usecases
    /*println("==== i. Get All column names=====")
    val colname = dfin1.dtypes.foreach(f=>println(f._1))
    println("====ii. Get All column with datatype====")
    val coldt = dfin1.dtypes.foreach(f=>println(f._1+","+f._2))
    println("========iii and iv. Get All Integer column with 10 records======")
    val integerColumnsre=dfin1.schema.fields.filter(_.dataType.isInstanceOf[IntegerType])
    dfin1.select(integerColumnsre.map(x=>col(x.name)):_*).show(10)*/
    
    // 24. Remove the rows
    val dfnull1 = dfin1.filter(col("IssuerId").isNotNull)
    //println(dfnull1.count())
    val dfnull2 = dfin2.filter(col("IssuerId").isNotNull)
    //println(dfnull2.count())

    //val dfnull1 = dfin1.na.drop()
    dfnull1.show()
    //println(dfnull1.count())
    //val dfnull2 = dfin2.na.drop()
    dfnull2.show()
    //println(dfnull2.count())
    
    val dfinmerge = dfnull1.union(dfnull2).distinct()
    //println("merged after removing nulls :")
    //dfinmerge.show(10)

   // 26. Import the package, instantiate the class and register the method
    val dfim = new allmethods
    val dfinsu =udf(dfim.remspecialchar _)
    spark.udf.register("dfinsu",dfim.remspecialchar _)
    
    // 27. Call the above udf in the DSL by passing NetworkName column as an argument
    //val dfnetwork = dfinmerge.withColumn("Ntwrkname",dfinsu(col("NetworkName"))).show()
    
    // 28. Save the DF in JSON into HDFS with
    //dfnetwork.write.format("json").mode("overwrite").save("hdfs://localhost:54310/user/hduser/sparkhack2output/insunetjson")
    //println("Saved JSON into HDFS")
    // 29. Save the DF in CSV into HDFS
    //dfnetwork.write.format("csv").mode("overwrite").option("delimiter","~").option("header","true").save("hdfs://localhost:54310/user/hduser/sparkhack2output/insunetcsv")
    //println("Saved CSV into HDFS")

    // 30. Save the DF into hive
    //dfnetwork.write.mode("append").option("path","hdfs://localhost:54318/user/hive/warehouse/").saveAsTable("insunethive")
    //println("Saved DF into hive")
    
    // Use RDD functions:
    // 31. Load the file custs_states.csv
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
    
    custfilterdf.filter("prof is not null")
    custfilterdf.where("prof is null")
    custfilterdf.na.drop()
    println("=============")
    statesfilterdf.filter("statecode is not null")
    statesfilterdf.where("statecode is null")
    statesfilterdf.na.drop()
    
    // Use SQL Queries:
    // 35. Register the above step 34 DFs as temporary views
    custfilterdf.createOrReplaceTempView("custview")
    statesfilterdf.createOrReplaceTempView("statesview")
    
    // 36. Register the DF generated in step 23.d as a tempview
    //dfinmerge.createOrReplaceTempView("insureview")
    
    // 37. Register udf
    val remspecialcharudf =udf(dfim.remspecialchar _)
    spark.udf.register("remspecialcharudf",dfim.remspecialchar _)
    
    // 38. Write an SQL query
    dfinmerge.createOrReplaceTempView("insureview")
    // a. Pass NetworkName
    spark.sql("""select IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,remspecialcharudf(NetworkName) 
      as cleannetworkname,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan from insureview""").show()
    // b. Add current date, current timestamp
    spark.sql("""select IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,
    MarketCoverage,DentalOnlyPlan,current_date() as curdt,current_timestamp() as curts from insureview""").show()
    // c. Extract the year and month from the businessdate
    //spark.sql("select IssuerId,IssuerId2,convert(varchar(10),convert(BusinessDate,getdate(),23),23),StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan from insureview").show()
    //spark.sql("select if (NetworkURL = 'http') elseif (NetworkURL = 'https') print 'http non secured : condition is true' else print 'noprotocol: condition is false' from insureview")
    /*dfinmerge.select(col("*"), (when(col("NetworkURL").startsWith("http:")=== true,"http non secured")
      .when(col("NetworkURL").startsWith("https:") === true,"http secured"))
      .otherwise("noprotocol").alias("protocol")).show()*/
      spark.sql("""select case when substring(NetworkURL,1,5) = 'http:' then 'http non secure' when substring 
      (NetworkURL,1,5) = 'https' then 'http secure' else 'noprotocol' end as protocol from insureview""").show()
      
      spark.sql("""select i.cleannetworkname,i.curdt,i.curts,i.protocol,c.age,c.prof,s.statecode from statesview s 
        inner join insureview i on (s.statecode = i.StateCode) 
        join custview c on i.custnum=c.custid""").show()
     
  }
  
}