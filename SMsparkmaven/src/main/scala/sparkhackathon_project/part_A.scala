package sparkhackathon_project
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Part A - SPARK CORE RDD TRANSFORMATIONS / ACTIONS
// PartA_1. Data cleaning, cleansing, scrubbing
object part_A {
  case class insureclass(IssuerId:Int,IssuerId2:Int,BusinessDate:String,StateCode:String,SourceName:String
      ,NetworkName:String,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)
  //case class insureclass1(IssuerId:Int,IssuerId2:Int,BusinessDate:String,StateCode:String,SourceName:String,NetworkName:String,NetworkURL:String,custnum:Int,MarketCoverage:String,DentalOnlyPlan:String)

  def main(args:Array[String]) : Unit =
  {
    val spark = SparkSession.builder().appName("PartA").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    // 1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata
    
    val insurance1 = sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv")
    //insurance1.foreach(println)
    
    
    // 2. Remove the header line from the RDD contains column names.
    
    val header = insurance1.first()
    val removeheader = insurance1.filter(x => x != header)
    
    
    val totallines = removeheader.count()
    
    
    // 3. Remove the Footer/trailer also which contains “footer count is 404”
    
    //val rddfooter = removeheader.filter(x => !x.contains("count"))
    //rddfooter.foreach(println)
    val rddfooter = removeheader.zipWithIndex()
    val counth = rddfooter.count
    val removefooter = rddfooter.filter( x => x._2 < counth -1 )
    val newdata = removefooter.map(x => x._1)
       
    
    // 4. Display the count and show few rows and check whether header and footer is removed.
    
    //println(s"Total lines after removing header ${removeheader.count()} and footer ${newdata.count()}")
    
    //println(newdata.first())
    
    
    // 5. Remove the blank lines in the rdd.
    
    val rddblank = newdata.filter(x => x.trim().length > 0)
    //rddblank.foreach(println)
    
    // 6. Map and split using ‘,’ delimiter.
    
    val rddmap = rddblank.map(x => x.split(",",-1).toList)
    //rddmap.foreach(println)
    // 7. Filter number of fields are equal to 10 columns only
    
    val rddfilter = rddmap.filter(x => (x.length == 10))
    //rddfilter.foreach(println)    
    
    // 8. Add case class namely insureclass
    
    val rddclass = rddfilter.map(x => insureclass (x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
    //rddclass.foreach(println)
    
    // 9. Take the count of the RDD created in step 7 and step 1
    
     val insurance1count = insurance1.count()
     val rddfiltercount = rddclass.count()

     println("Lines of  input file : " + insurance1count)
     println("Lines of cleaned up file : " + rddfiltercount)
     println("Total rows are removed in the cleanup process : " + (insurance1count - rddfiltercount))
    // 10. Create another RDD namely rejectdata and Add new column called number of columns from reject row
    
    val rejectdata = rddmap.filter(x => (x.length != 10))
    //rejectdata.foreach(println)
    
    val numcols = rejectdata.map(x => (x.length,x(0)))
    //numcols.foreach(println)
    
    // 11. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata
    
    val insurance2 = sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
    //insurance2.foreach(println)
    val replace = insurance2.map(x => x.replace("01-10-2019", "2019-10-01"))
    val replace2 = replace.map(x => x.replace("02-10-2019", "2019-10-02"))
     
    // 12. Remove the header line from the RDD contains column names.
    // 2.remove header
    val header1 = replace2.first()
    val removeheader1 = replace2.filter(x => x != header1)
    //removeheader1.foreach(println)
    
    
    val totallines1 = removeheader1.count()
    //println(totallines1)
    
    // 3. Remove the Footer/trailer also which contains “footer count is 404”
    
    val rddfooter1 = removeheader1.zipWithIndex()
    val count1 = rddfooter1.count
    val removefooter1 = rddfooter1.filter( x => x._2 < count1 -1 )
    val newdata1 = removefooter1.map(x => x._1)
    // (or)
    //val rddfooter1 = removeheader1.filter(x => !x.contains("count"))
    //rddfooter1.foreach(println)
    
    // 4. Display the count and show few rows and check whether header and footer is removed.
    
    //println(s"Total lines after removing header ${removeheader1.count()} and footer ${newdata1.count()}")      
    //println(newdata1.first())
    
    // 5. Remove the blank lines in the rdd.
    
    val rddblank1 = newdata1.filter(x => x.trim().length > 0)
   
    //rddblank1.foreach(println)
    
    
    // 6. Map and split using ‘,’ delimiter.
    
    val rddmap1 = rddblank1.map(x => x.split(",",-1))
    
    // 7. Filter number of fields are equal to 10 columns only

    //println("In the header we have 10 columns so we remove others")
    val rddfilter1 = rddmap1.filter(x => (x.length == 10))
    
    // 8. Add case class
    val removerows = rddfilter1.filter(x => x(0)!= "" || x(1) != "")
    val rejectdata1 = removerows.map(x => insureclass(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
    //rejectdata1.foreach(println)
     
    // 9. Take the count of the RDD
     val insurance2count = insurance2.count()
     val rejectdata1count = rejectdata1.count()

     println("Lines of  input file : " + insurance2count)
     println("Lines of cleaned up file : " + rejectdata1count)
     println("Total rows are removed in the cleanup process : " + (insurance2count - rejectdata1count))

    // PartA_2. Data merging, Deduplication, Performance Tuning & Persistance
   
    
    // 13. Merge  
    val insuredatamerged = rddclass.union(rejectdata1)
    //insuredatamerged.foreach(println)
    val countmerged = insuredatamerged.count()
   
    
    // 14. Persist
    val rddpersist = insuredatamerged.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
    val persistcount = rddpersist.count()
    
    // 15. Calculate the count of rdds
     println("Count of first file rdd : " + rddfiltercount)
     println("Count of second file rdd : " + rejectdata1count)
     println("Count of both first and second file rdd : " + (rddfiltercount + rejectdata1count))
     println("Count of persist file rdd : " + persistcount)
    
     // 16. Remove duplicates
     val removeduplicates = insuredatamerged.distinct()
     val countremoveddup = removeduplicates.count()
     
     println("Count of before remove duplicates: " + countmerged)
     println("Count of After remove duplicates : " + countremoveddup)
     println("Total duplicates in mergeddata : " + (countmerged - countremoveddup))
    
    // 17. Increase the number of partitions in the above rdd
     val insuredatarepart = removeduplicates.repartition(8)
     //insuredatarepart.foreach(println)
     
     //18. Split the above RDD using the businessdate field
     val rdd_20191001 = insuredatarepart.filter(x => x.BusinessDate == "2019-10-01")
     rdd_20191001.foreach(println)
     val rdd_20191002 = insuredatarepart.filter(x => x.BusinessDate == "2019-10-02")
     rdd_20191002.foreach(println)
     
     // 19. Store the RDDs created into HDFS locations.
     
     rejectdata.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2output/insurance1output.csv")
     numcols.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2output/countcoloutput.csv")
     insuredatamerged.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2output/mergedataoutput.csv")
     rdd_20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2output/bussinessdate01output.csv")
     rdd_20191002.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2output/bussinessdate02output.csv")
     println("DataStored into hdfs")
     
     // 20. Convert the RDD created into Dataframe
     import spark.implicits._
     val insuredaterepartdf = insuredatarepart.toDF()
     insuredaterepartdf.show(100,false)
     insuredaterepartdf.printSchema()


     
     
     
     
    
}
}