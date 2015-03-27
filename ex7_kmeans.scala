import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

val numIterations = 10
val allK  = 1 to 5
var WSSSE = Array(0.0,0.0,0.0,0.0,0.0)

val data = sc.textFile("ex7.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

for(numClusters <- allK) {
  val clusters = KMeans.train(parsedData, numClusters, numIterations)
  WSSSE(numClusters-1) = clusters.computeCost(parsedData)
}

println("Within Set Sum of Squared Errors = " + WSSSE(numClusters-1))

var bestK = 3
val clusters = KMeans.train(parsedData, bestK, numIterations)

