import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession


Object ranking_bike_stations_using_pageRank {
	def main(args:Array[String]){

		// creating SparkSession
		val spark = SparkSession.builder.appName("rankStations").getOrElse()

		import spark.implicits._

		// loading csv file into dataframe
		val df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/2017-fordgobike-tripdata.csv")

		// printing count and schema
		println(s"total count is : ${df.count()}")
		df.printSchema()

		// creating dataframe of only id and name of stations
		val justStations = df.selectExpr("float(start_station_id) as station_id","start_station_name").distinct()

		// getting all station's distinct ids
		val stations = df.select("start_station_id","end_station_id").rdd.distinct().flatMap( x => Iterable(x(0).asInstanceOf[Number].longValue,x(1).asInstanceOf[Number].longValue)).distinct().toDF()

		// creating vertices for graph
		val stationVertices:RDD[(VertexId,String)] = stations.join(justStations,stations("value") === justStations("station_id")).select("station_id","start_station_name").rdd.map(row => (row(0).asInstanceOf[Number].longValue,row(1).asInstanceOf[String]))

		// creating edges for graph
		val stationEdge: RDD[Edge[Long]] = df.select("start_station_id","end_station_id").rdd.map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1))

		// setting default values to return incase if vertex not found
		val defaultStation = ("Missing Station")

		// creating graph and caching
		val stationGraph = Graph(startVertices,stationEdge,defaultStation)	
		stationGraph.cache()

		// printing number of vertices and edges
		println("number of vertices are : " + stationGraph.numVertices)
		println("number of edges are : " + stationGraph.numEdges)

		// applying pageRank algorithm : dynamic pageRank
		val ranks = stationGraph.pageRank(0.0001).vertices

		// join rank and stationGraph and taking top 10 stations by ranking 
		ranks.join(stationGraph).sortBy(_._2._1,ascending=false).take(10).foreach(x => println(x._2._2))
		
		// total trips between the stations
		stationGraph.groupEdges((edge1, edge2) => edge1 + edge2).triplets.sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toString + "trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ". ").take(10).foreach(println)

		// indegree
		stationGraph.inDegrees.join(stationVertices).sortBy(_._2._1 , ascending=false).take(10).foreach(x => println(x._2._2 + "has " + x._2._1 + "in Degrees. "))

		// outdegree
		stationGraph.outDegrees.join(stationVertices).sortBy(_._2._1 , ascending=false).take(10).foreach(x => println(x._2._2 + "has " + x._2._1 + "in Degrees. "))	
	}
}