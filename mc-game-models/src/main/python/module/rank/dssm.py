import tensorflow as tf
import numpy as np
from tensorflow.python.ops import array_ops
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow_addons as tfa
from rank.losses import alpha_weights

from absl import flags, logging

FLAGS = flags.FLAGS


# class Tower(keras.Model):
#     def __init__(self, units=[128,128,128], use_batch_norm=True):
#         super().__init__()
#


class TowerModelBN(keras.Model):
    def __init__(self):
        super().__init__()
        self.bn1 = layers.BatchNormalization()
        self.dense1 = layers.Dense(units=256)
        self.bn2 = layers.BatchNormalization()
        self.hidden1 = layers.ReLU()
        self.dense2 = layers.Dense(units=256)
        self.bn3 = layers.BatchNormalization()
        self.hidden2 = layers.ReLU()
        self.dense = layers.Dense(units=128, activation='tanh')

    def call(self, inputs, training=False):
        x = self.bn1(inputs, training=False)
        x = self.dense1(x)
        x = self.bn2(x, training=False)
        x = self.hidden1(x)
        x = self.dense2(x)
        x = self.bn3(x, training=False)
        x = self.hidden2(x)
        output = self.dense(x)
        return output


class TowerModel(keras.Model):
    def __init__(self):
        super().__init__()
        self.dense1 = layers.Dense(units=128)
        self.hidden1 = layers.ReLU()
        self.dense2 = layers.Dense(units=128)
        self.hidden2 = layers.ReLU()
        self.dense = layers.Dense(units=128, activation='tanh')

    def call(self, inputs, training=False):
        x = self.dense1(inputs)
        x = self.hidden1(x)
        x = self.dense2(x)
        x = self.hidden2(x)
        output = self.dense(x)
        return output


def cosine_similarity(a, b):
    a_norm = tf.nn.l2_normalize(a, axis=1)
    b_norm = tf.nn.l2_normalize(b, axis=1)
    return tf.reduce_sum(tf.multiply(a_norm, b_norm), axis=1, keepdims=True)


def build_pred(item, user_context):
    with tf.name_scope("similarity_layer"):
        return tf.identity((1 + cosine_similarity(item, user_context)) / 2, name='PRED')


def build_tower(features, group, model, params, training):
    #     group_features = {}
    #     for feature_name, feature in params["feature_table"].items():
    #         if group == feature.group:
    #             group_features[feature_name] = features[feature_name]

    columns = params["user_context_columns"] if group == "USER_CONTEXT" else params["item_columns"]
    feature_layer = layers.DenseFeatures(columns)
    emb_input = feature_layer(features)
    logging.info("%s input dim: %d", group, emb_input.shape[-1])
    output = model(emb_input, training)
    return output


def build_model(features, user_context_model, item_model, params, training=False):
    user_context_tower = build_tower(features, "USER_CONTEXT", user_context_model, params, training)
    item_tower = build_tower(features, "ITEM", item_model, params, training)
    pred = build_pred(item_tower, user_context_tower)
    return pred, item_tower, user_context_tower


def model_fn(features, labels, mode, params):
    training = (mode == tf.estimator.ModeKeys.TRAIN)

    if FLAGS.use_bn:
        logging.info("use BatchNormalization")
        user_context_model = TowerModelBN()
        item_model = TowerModelBN()
    else:
        user_context_model = TowerModel()
        item_model = TowerModel()

    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {}
        pred, item, user_context = build_model(features, user_context_model, item_model, params, training)
        predictions = {
            'PRED': pred,
            'ITEM': tf.nn.l2_normalize(item, axis=1, name='ITEM'),
            'USER_CONTEXT': tf.nn.l2_normalize(user_context, axis=1, name='USER_CONTEXT')
        }
        return tf.estimator.EstimatorSpec(mode, predictions=predictions)

    # labels = tf.expand_dims(labels, axis=1)
    loss_weights = alpha_weights(labels, alpha=0.8)

    with tf.GradientTape() as tape:
        pred, item, user_context = build_model(features, user_context_model, item_model, params, training)
        loss = tf.compat.v1.losses.log_loss(labels, pred, weights=loss_weights)
    if 'optimizer' not in params:
        learning_rate = tf.compat.v1.train.exponential_decay(FLAGS.learning_rate,
                                                             tf.compat.v1.train.get_global_step(), FLAGS.decay_steps,
                                                             FLAGS.decay_coef, staircase=True)
        optimizer = tfa.optimizers.LazyAdam(learning_rate)
        optimizer.iterations = tf.compat.v1.train.get_or_create_global_step()
    else:
        optimizer = params['optimizer']
    metrics = {
        'auc': tf.compat.v1.metrics.auc(labels, pred),
        'label/mean': tf.compat.v1.metrics.mean(labels),
        'prediction/mean': tf.compat.v1.metrics.mean(pred)
    }

    if mode == tf.estimator.ModeKeys.EVAL:
        return tf.estimator.EstimatorSpec(mode, loss=loss, eval_metric_ops=metrics)

    trainable_variables = user_context_model.trainable_variables + item_model.trainable_variables
    gradients = tape.gradient(loss, trainable_variables)
    optimize = optimizer.apply_gradients(zip(gradients, trainable_variables))
    update_ops = tf.compat.v1.get_collection(tf.compat.v1.GraphKeys.UPDATE_OPS)
    train_op = tf.group([optimize, update_ops])
    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=train_op)
