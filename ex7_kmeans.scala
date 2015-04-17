import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

val numIterations = 10
val allK  = 1 to 5
var WSSSE = Array(0.0,0.0,0.0,0.0,0.0)

val data = sc.textFile("ex7.txt")
println("%table x\ty\n" + data.map { s => s.split(" ").mkString("\t") }.collect().mkString("\n"))

val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

for(numClusters <- allK) {
  val clusters = KMeans.train(parsedData, numClusters, numIterations)
  WSSSE(numClusters-1) = clusters.computeCost(parsedData)
}

println("%table #\tWithin Set Sum of Squared Errors\n" + WSSSE.zipWithIndex.map(p => p._2 + "\t" + p._1).mkString("\n"))

var bestK = 3
val clusters = KMeans.train(parsedData, bestK, numIterations)

