// chapter 2 of advanced analytics with spark:
val rawblocks = sc.textFile("/home/clc31/src/spark-examples/data/donation/linkage")

val head = rawblocks.take(10)

head.foreach(println)

def isHeader(line: String) = line.contains("id_1")

head.filter(isHeader).foreach(println)

head.filterNot(isHeader).length

head.filter(x => !isHeader(x)).length

head.filter(!isHeader(_)).length

val noheader = rawblocks.filter(x => !isHeader(x))

val line = head(5)
val pieces = line.split(',')

val id1 = pieces(0).toInt
val id2 = pieces(1).toInt
val matched = pieces(11).toBoolean

val rawscores = pieces.slice(2, 11)
def toDouble(s: String) = {
  if ("?".equals(s)) Double.NaN else s.toDouble
}
val scores = rawscores.map(toDouble)
scores.map(s => s.toDouble)

def parse(line: String) = {
  val pieces = line.split(',')
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val scores = pieces.slice(2, 11).map(toDouble)
  val matched = pieces(11).toBoolean
  (id1, id2, scores, matched)
}
val tup = parse(line)

tup._1
tup.productElement(0)
tup.productArity

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

def parse(line: String) = {
  val pieces = line.split(',')
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val scores = pieces.slice(2, 11).map(toDouble)
  val matched = pieces(11).toBoolean
  MatchData(id1, id2, scores, matched)
}
val md = parse(line)
val id1 = pieces(0).toInt
val id2 = pieces(1).toInt

val mds = head.filter(x => !isHeader(x)).map(x => parse(x))

val parsed = noheader.map(line => parse(line))
parsed.cache()

val grouped = mds.groupBy(md => md.matched)
grouped.mapValues(x => x.size).foreach(println)

val matchCounts = parsed.map(md => md.matched).countByValue()

val matchCountsSeq = matchCounts.toSeq
matchCountsSeq.sortBy(_._1).foreach(println)
matchCountsSeq.sortBy(_._2).foreach(println)
matchCountsSeq.sortBy(_._2).reverse.foreach(println)

parsed.map(md => md.scores(0)).stats()

import java.lang.Double.isNaN
parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()

val stats = (0 until 9).map(i => {
  parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
})
stats.foreach(println)

import org.apache.spark.util.StatCounter
class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0
  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }
  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }
  override def toString = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}
object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}
import org.apache.spark.rdd.RDD
def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
  val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
    val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
    iter.foreach(arr => {
      nas.zip(arr).foreach { case (n, d) => n.add(d) }
    })
    Iterator(nas)
  })
  nastats.reduce((n1, n2) => {
    n1.zip(n2).map { case (a, b) => a.merge(b) }
  })
}

// not sure the above actually works correctly (see warning about :paste)
val nastats = NAStatCounter(17.29)
val nas1 = NAStatCounter(10.0)
nas1.add(2.1)
val nas2 = NAStatCounter(Double.NaN)
nas1.merge(nas2)

val arr = Array(1.0, Double.NaN, 17.29)
val nas = arr.map(d => NAStatCounter(d))

val nasRDD = parsed.map(md => {
  md.scores.map(d => NAStatCounter(d))
})

val nas1 = Array(1.0, Double.NaN).map(d => NAStatCounter(d))
val nas2 = Array(Double.NaN, 2.0).map(d => NAStatCounter(d))
val merged = nas1.zip(nas2).map(p => p._1.merge(p._2))

val merged = nas1.zip(nas2).map { case (a, b) => a.merge(b) }

val nas = List(nas1, nas2)
val merged = nas.reduce((n1, n2) => {
  n1.zip(n2).map { case (a, b) => a.merge(b) }
})

// seems to bomb out here:
val reduced = nasRDD.reduce((n1, n2) => {
  n1.zip(n2).map { case (a, b) => a.merge(b) }
})
reduced.foreach(println)

val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

statsm.zip(statsn).map { case(m, n) =>
  (m.missing + n.missing, m.stats.mean - n.stats.mean)
}.foreach(println)

def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
case class Scored(md: MatchData, score: Double)
val ct = parsed.map(md => {
  val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
  Scored(md, score)
})
ct.filter(s => s.score >= 4.0).map(s => s.md.matched).countByValue()


