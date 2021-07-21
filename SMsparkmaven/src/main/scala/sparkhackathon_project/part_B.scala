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
          StructField("BusinessDate",DateType,true), 
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
            dfin2.show()
            
// 23. Apply the below DSL functions
            
    // a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.
    val dfrename = dfin1.select(col("IssuerId"),col("IssuerId2"),col("BusinessDate"),col("StateCode").alias("stcd")
    ,col("SourceName").alias("srcnm"),col("NetworkName"),col("NetworkURL"),col("custnum"),col("MarketCoverage")
    ,col("DentalOnlyPlan"))
    val dfrename1 = dfin2.select(col("IssuerId"),col("IssuerId2"),col("BusinessDate"),col("StateCode").alias("stcd")
        ,col("SourceName").alias("srcnm"),col("NetworkName"),col("NetworkURL"),col("custnum"),col("MarketCoverage")
        ,col("DentalOnlyPlan"))
        
    // b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field 
    val dfconcat =dfrename.select(col("*"),concat(col("IssuerId"),lit(" "),col("IssuerId2")).as("issueridcomposite"))
    val dfconcat1 =dfrename1.select(col("*"),concat(col("IssuerId"),lit(" "),col("IssuerId2")).as("issueridcomposite"))

    // c. Remove DentalOnlyPlan column
    val dfdrop = dfconcat.drop("DentalOnlyPlan")
    val dfdrop1 = dfconcat1.drop("DentalOnlyPlan")

    // d. Add columns that should show the current system date and timestamp with the 
    val dfcurdatets = dfdrop.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())
    val dfcurdatets1 = dfdrop1.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())
        
    // i,ii,iii,iv usecases
    /*println("==== i. Get All column names=====")
    val colname = dfin1.dtypes.foreach(f=>println(f._1))
    println("====ii. Get All column with datatype====")
    val coldt = dfin1.dtypes.foreach(f=>println(f._1+","+f._2))
    println("========iii and iv. Get All Integer column with 10 records======")
    val integerColumnsre=dfin1.schema.fields.filter(_.dataType.isInstanceOf[IntegerType])
    dfin1.select(integerColumnsre.map(x=>col(x.name)):_*).show(10)*/
    
    // 24. Remove the rows
    val dfnull1 = dfcurdatets.filter(col("IssuerId").isNotNull)
    //dfnull1.show()
    //println(dfnull1.count())
    val dfnull2 = dfcurdatets.filter(col("IssuerId").isNotNull)
    //dfnull2.show()
    //println(dfnull2.count())

    //val dfnull1 = dfcurdatets.na.drop()
    //dfnull1.show(100)
    //println(dfnull1.count())
    //val dfnull2 = dfcurdatets.na.drop()
    //dfnull2.show(100)
    //println(dfnull2.count())
    
    val dfinmerge = dfnull1.union(dfnull2).distinct()
    println("merged after removing nulls :")
    dfinmerge.show(400)

   // 26. Import the package, instantiate the class and register the method
    val dfim = new allmethods
    val dfinsu =udf(dfim.remspecialchar _)
    spark.udf.register("dfinsu",dfim.remspecialchar _)
    
    // 27. Call the above udf in the DSL by passing NetworkName column as an argument
    val dfnetwork = dfinmerge.withColumn("Ntwrkname",dfinsu(col("NetworkName")))
    
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
    //println("========custs data=========")
    val custsplit = custstates.map(x => x.split(","))
    val custfilter = custsplit.filter(x => (x.length == 5))
    val custmap = custfilter.map(x => (x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    //custmap.foreach(println)
    
    //println("=========state data==========")
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
    
    
    val statesfilterdf = statemap.toDF("stated","statedesc")
    //statesfilterdf.show()
    //statesfilterdf.printSchema()
    
    
    val cnull = custfilterdf.na.drop()
    //println(cnull.count())
    println("=============")
    val snull = statesfilterdf.na.drop()
    //println(snull.count())
    
    // Use SQL Queries:
    // 35. Register the above step 34 DFs as temporary views
    cnull.createOrReplaceTempView("custview")
    snull.createOrReplaceTempView("statesview")
    
    // 36. Register the DF generated in step 23.d as a tempview
    //dfinmerge.createOrReplaceTempView("insureview")
    
    // 37. Register udf
    val remspecialcharudf =udf(dfim.remspecialchar _)
    spark.udf.register("remspecialcharudf",dfim.remspecialchar _)
    
    // 38. Write an SQL query
    dfinmerge.createOrReplaceTempView("insureview")
    //spark.conf.set("spark.sql.shuffle.partitions",4)
    //println(dfinmerge.groupBy("IssuerId").count().rdd.partitions.length)
    
    // a. Pass NetworkName
    val NN = spark.sql("""select IssuerId,IssuerId2,BusinessDate,stcd,srcnm,NetworkName,remspecialcharudf(NetworkName) 
      as cleannetworkname,NetworkURL,custnum,MarketCoverage,issueridcomposite from insureview""")
      //NN.show()
      
    // b. Add current date, current timestamp
    val DT = NN.withColumn("curdt",current_date()).withColumn("curts",current_timestamp())
    DT.show()
    
    println("// c. Extract the year and month from the businessdate")
    val CYM = DT.select(col("*"),to_date(col("BusinessDate"), "MM/dd/yyyy").alias("Business_Date"))
    CYM.show()
    val CYM1 = CYM.select(col("*"),year(col("Business_Date")).as("yr"),month(col("Business_Date")).as("mth"))
    CYM1.show()
    val YM = CYM1.drop("BusinessDate")
    YM.show()
    
    // d. Extract from the protocol 
    //val pro = spark.sql("""select case when substring(NetworkURL,1,5) = 'http:' then 'http non secure' when substring 
      //(NetworkURL,1,5) = 'https' then 'http secure' else 'noprotocol' end as protocol from insureview""")
    val pro = YM.select(col("*"),(when(col("NetworkURL").startsWith("http:")=== true,"http non secured")
      .when(col("NetworkURL").startsWith("https:") === true,"http secured"))
      .otherwise("noprotocol").alias("protocol"))
    pro.show()
    
    // e. Display all the columns  
    pro.createOrReplaceTempView("insureview1")
    val dfs = spark.sql("select * from insureview1")
    println("top of tempview : ")
    dfs.show()
   
    /*val joinfile = spark.sql("""select i.IssuerId,i.IssuerId2,i.Business_Date,i.yr,i.mth,i.stcd,i.srcnm,i.NetworkName,
      i.cleannetworkname,i.NetworkURL,i.custnum,i.MarketCoverage,i.issueridcomposite,i.curdt,i.curts,i.protocol,
      c.age,c.prof,s.stated from insureview1 i inner join statesview s on i.stcd = s.stated join 
      custview c on i.custnum=c.custid""")
      joinfile.show()*/
      
    val joinfile1 = spark.sql("""select * from insureview1 i inner join statesview s on i.stcd = s.stated join
      custview c on i.custnum = c.custid""")
    joinfile1.show()
     
    // 39. Store DF in Parquet formats into HDFS location
    //joinfile.coalesce(1).write.format("parquet").mode("overwrite").save("hdfs://localhost:54310/user/hduser/sparkhack2output/joinparquet")
    //println("Saved parquet into HDFS")
    
    // 40. Write an SQL query
    joinfile1.createOrReplaceTempView("insurinfo")
    spark.sql("select * from insurinfo").show()
    val insuagg = spark.sql("""select avg(age) as avgage,count(statedesc) as count,prof from insurinfo 
      order by prof desc""")
    /*val insuagg = joinfile1.groupBy("protocol")
                        .agg(avg(col("age"))
                        .alias("Avgage"))
                        .groupby(count(col("statedesc")))
                        .orderBy(count(col("protocol").desc))
                        .show()*/
    // 41. Store the DF into MYSQL table
    insuagg.write.format("jdbc").option("url","jdbc:mysql://localhost/custdb")
    .mode("overwrite")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","insureaggregated")
    .option("driver","com.mysql.cj.jdbc.Driver").save()
    
    
    
    
    
    
     
  }
  
}