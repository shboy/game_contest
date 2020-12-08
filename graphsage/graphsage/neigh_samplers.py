from __future__ import division
from __future__ import print_function

from graphsage.layers import Layer

import tensorflow as tf
flags = tf.app.flags
FLAGS = flags.FLAGS


"""
Classes that are used to sample node neighborhoods
"""

class UniformNeighborSampler(Layer):
    """
    Uniformly samples neighbors. 均匀取样
    Assumes that adj lists are padded with random re-sampling

    01. 第一次call：传入ids(shape: [batch_size])， lookup出这些id的邻接矩阵， 每个随机选取10个， 返回adj_lists(shape: [batch_size, 10])
    02. 第二次call：传入ids(shape: [batch_size*10])， lookup出这些的邻接矩阵，每个随机选取25个， 返回adj_lists(shape: [batch_size, 250])
    """
    def __init__(self, adj_info, **kwargs):
        super(UniformNeighborSampler, self).__init__(**kwargs)
        self.adj_info = adj_info # <tf.Variable 'adj_info:0' shape=(14756, 100) dtype=int32_ref>

    def _call(self, inputs):
        ids, num_samples = inputs # num_samples: 10, 25
        # adj_info(邻接矩阵): (14756, 100): 每个节点取了100个邻居
        # ids: (batch_size, ) -> (batch_size, 100) (batch_size*10, ) -> (batch_size*10, 100)
        adj_lists = tf.nn.embedding_lookup(self.adj_info, ids)
        # tf.random_shuffle: Randomly shuffles a tensor along its first dimension 所以要转置
        # (batch_size, 100) => (100, batch_size) => random_shuffle => (batch_size, 100)
        # (batch_size*10, 100) => (100, batch_size*10) => random_shuffle => (batch_size*10, 100)
        adj_lists = tf.transpose(tf.random_shuffle(tf.transpose(adj_lists)))
        # (batch_size, 100) => (batch_size, 10)
        # (batch_size*10, 100) => (batch_size*10, 25)
        adj_lists = tf.slice(adj_lists, [0,0], [-1, num_samples])
        return adj_lists
