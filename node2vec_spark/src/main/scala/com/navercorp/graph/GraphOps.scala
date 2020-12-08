package com.navercorp.graph

import scala.collection.mutable.ArrayBuffer

object GraphOps {
  /**
   *
   * @param nodeWeights
   * @return
    *
   * 每一列中最多只有两种事件
   * J: 里面储存着第i列不是事件i的另外一个事件的标号
   * q: 里面存着第i列对应的事件i矩形站的面积百分比
   */
  def setupAlias(nodeWeights: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val K = nodeWeights.length
    val J = Array.fill(K)(0)
    val q = Array.fill(K)(0.0)

    val smaller = new ArrayBuffer[Int]()
    val larger = new ArrayBuffer[Int]()

    val sum = nodeWeights.map(_._2).sum
    nodeWeights.zipWithIndex.foreach { case ((nodeId, weight), i) =>
      q(i) = K * weight / sum
      if (q(i) < 1.0) {
        smaller.append(i)
      } else {
        larger.append(i)
      }
    }

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.remove(smaller.length - 1)
      val large = larger.remove(larger.length - 1)

      J(small) = large
      q(large) = q(large) + q(small) - 1.0 // q(small) 的值没有变
      if (q(large) < 1.0) smaller.append(large)
      else larger.append(large)
    }

    (J, q)
  }

  def setupEdgeAlias(p: Double = 1.0, q: Double = 1.0)(srcId: Long, srcNeighbors: Array[(Long, Double)], dstNeighbors: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val neighbors_ = dstNeighbors.map { case (dstNeighborId, weight) =>
      var unnormProb = weight / q
      if (srcId == dstNeighborId) unnormProb = weight / p
      else if (srcNeighbors.exists(_._1 == dstNeighborId)) unnormProb = weight

      (dstNeighborId, unnormProb)
    }

    setupAlias(neighbors_)
  }
  
  /**
   *
   * @param J 里面储存着第i列不是事件i的另外一个事件的标号
   * @param q 里面存着第i列对应的事件i矩形站的面积百分比
   * @return
   */
  def drawAlias(J: Array[Int], q: Array[Double]): Int = {
    val K = J.length
    val kk = math.floor(math.random * K).toInt

    if (math.random < q(kk)) kk
    else J(kk)
  }

  lazy val createUndirectedEdge: (Long, Long, Double) => Array[(Long, Array[(Long, Double)])] = (srcId: Long, dstId: Long, weight: Double) => {
    Array(
      (srcId, Array((dstId, weight))),
      (dstId, Array((srcId, weight)))
    )
  }
  
  lazy val createDirectedEdge: (Long, Long, Double) => Array[(Long, Array[(Long, Double)])] = (srcId: Long, dstId: Long, weight: Double) => {
    Array(
      (srcId, Array((dstId, weight)))
    )
  }
}
