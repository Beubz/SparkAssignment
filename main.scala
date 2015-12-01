import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object Assignment {

	case class Crime(date:String , adress:String , district:Int , beat:String , grid:Int, crimedescr:String , ucrCode:Int, latitude:String , longitude:String )

	def main(args: Array[String]){

		val conf = new SparkConf().setAppName("Assignment").setMaster("local")
		val sc = new SparkContext(conf)

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		// this is used to implicitly convert an RDD to a DataFrame.
		import sqlContext.implicits._

		val file = sc.textFile("crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

		val crimes = file.map(line => {
			val l=line.split(",")
			Crime(l(0),l(1),l(2).toInt,l(3),l(4).toInt,l(5),l(6).toInt, l(7), l(8))
			})

		val crimesDF = crimes.toDF()

		var input="nul"

		while (input != 0)
		{
			println("What do you want to know ? \n")
			println("1. The crime that happens the most in Sacramento ? (RDD Style)")
			println("2. The 3 days with the highest crime count ?")
			println("3. The average of each crime per day ?")
			println("4. What is the crime that happens the most in Sacramento ? (DF style")
			println("5. The 3 days with the highest crime count ?")
			println("6. The average of each crime per day ? \n")
			println("0. QUIT \n")
			
			input = readLine("Choose wisely > ")

				input match{
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
					case "3" =>
					{

					}

					case "4" =>
					{
						val result = crimesDF.groupBy('crimedescr).count().sort('count.desc)
						result.withColumnRenamed("crimedescr", "Crime").show(1)
					}

					case "5" =>
					{
						val result = crimesDF.groupBy('date.substr(0, 7)).count().sort('count.desc)
						result.withColumnRenamed("substring(date,0,7)", "Date").withColumnRenamed("count", "Crime Count").show(3)
					}

					case "6" =>
					{
						val crimesCount = crimesDF.groupBy('crimedescr).count()
						val day = crimesDF.groupBy('date.substr(0, 7)).count()
						val result = crimesCount.select(crimesCount("crimedescr"), crimesCount("count")/day.count)
						result.withColumnRenamed("crimedescr", "Crime").withColumnRenamed("(count / 31)", "Per Day").show()

					}
				}		
			}
		}

}