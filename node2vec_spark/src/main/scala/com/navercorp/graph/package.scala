package com.navercorp

import java.io.Serializable

package object graph {
  
  /**
   *
   * @param neighbors 节点的邻居集合Array，每个邻居是一个二元组（nodeId, weight）
   * @param path 采样节点序列id
   */
  case class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                      var path: Array[Long] = Array.empty[Long]) extends Serializable
  
  /**
   *
   * @param dstNeighbors dst节点的邻居
   * @param J dstNeighbors是dst节点的邻居，J和q是dst节点作概率采样的Alias Table
   * @param q
   */
  case class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
                      var J: Array[Int] = Array.empty[Int],
                      var q: Array[Double] = Array.empty[Double]) extends Serializable
}
