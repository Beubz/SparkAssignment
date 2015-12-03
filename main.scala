import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv


object Assignment {

    //Class for DF
    case class Crime(date:String , adress:String , district:Int , beat:String , grid:Int, crimedescr:String , ucrCode:Int, latitude:String , longitude:String )

    def main(args: Array[String]){

        // SparkConf
        val conf = new SparkConf().setAppName("Assignment").setMaster("local")
        val sc = new SparkContext(conf)

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        // this is used to implicitly convert an RDD to a DataFrame.
        import sqlContext.implicits._

        //Open the CSV file , must be in root directory
        val file = sc.textFile("crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

        //Implement class with datas
        val crimes = file.map(line => {
            val l=line.split(",")
            Crime(l(0),l(1),l(2).toInt,l(3),l(4).toInt,l(5),l(6).toInt, l(7), l(8))
            })

        // to Data Frames
        val crimesDF = crimes.toDF()

        //Var for user choice
        var input="nul"

        while (input != 0)
        {
            println("\nWhat do you want to know ? \n")
            println("1. The crime that happens the most in Sacramento ? (RDD)")
            println("2. The 3 days with the highest crime count ? (RDD)")
            println("3. The average of each crime per day ? (RDD)")
            println("4. What is the crime that happens the most in Sacramento ? (DF)")
            println("5. The 3 days with the highest crime count ? (DF)")
            println("6. The average of each crime per day ? (DF)\n")
            println("7. Export to CSV \n")
            println("0. QUIT \n")

            input = readLine("Choose wisely > ")

            input match{
                // crime that happens the most in Sacramento with RDD
                case "1" => 
                {
                    file.mapPartitions(lines => {
                        val parser=new CSVParser(',')
                        lines.map(line => {
                            val columns = parser.parseLine(line)
                            Array(columns(5)).mkString(",")
                            })
                        }).countByValue().toList.sortBy(-_._2).take(1).foreach(println)
                }

                // 3 days with the highest crime count with RDD
                case "2" =>
                {
                    file.mapPartitions(lines => {
                        val parser=new CSVParser(',')
                        lines.map(line => {
                            val columns = parser.parseLine(line)
                            Array(columns(0).split(" ")(0)).mkString(",")
                            })
                        }).countByValue().toList.sortBy(-_._2).take(3).foreach(println)
                }

                // average of each crime per day with RDD
                case "3" =>
                {
                    file.mapPartitions(lines => {
                        val parser=new CSVParser(',')
                        lines.map(line => {
                            val columns = parser.parseLine(line)
                            Array(columns(5)).mkString(",")
                            })
                        }).countByValue().toList.sortBy(-_._2).map{ case(x,y) => (x, y/31)}.foreach(println)
                }

                // crime that happens the most in Sacramento with DF
                case "4" =>
                {
                    val result = crimesDF.groupBy('crimedescr).count().sort('count.desc)
                    result.withColumnRenamed("crimedescr", "Crime").show(1)
                }

                // 3 days with the highest crime count with DF
                case "5" =>
                {
                    val result = crimesDF.groupBy('date.substr(0, 7)).count().sort('count.desc)
                    result.withColumnRenamed("substring(date,0,7)", "Date").withColumnRenamed("count", "Crime Count").show(3)
                }

                // average of each crime per day with DF
                case "6" =>
                {
                    val crimesCount = crimesDF.groupBy('crimedescr).count()
                    val days = crimesDF.groupBy('date.substr(0, 7)).count()
                    val result = crimesCount.select(crimesCount("crimedescr"), crimesCount("count")/days.count)
                    result.withColumnRenamed("crimedescr", "Crime").withColumnRenamed("(count / 31)", "Per Day").show()

                }

                // Export to CSV
                case "7" =>
                {
                    val crimesCount = crimesDF.groupBy('crimedescr).count()
                    val days = crimesDF.groupBy('date.substr(0, 7)).count()
                    val result = crimesCount.select(crimesCount("crimedescr"), crimesCount("count")/days.count)
                    val csv = result.withColumnRenamed("crimedescr", "Crime").withColumnRenamed("(count / 31)", "Per Day")
                    csv.rdd.map(x=>x.mkString(",")).coalesce(1).saveAsTextFile("csvFile")

                }
            }       
        }
    }
}