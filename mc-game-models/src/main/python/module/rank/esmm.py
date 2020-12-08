import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow_addons as tfa
from rank.losses import alpha_weights
from absl import flags, logging
from rank.util import stat_model_parameters, DNNModel

# refer to: https://stackoverflow.com/questions/33662648/tensorflow-causes-logging-messages-to-double
#           https://stackoverflow.com/questions/19561058/duplicate-output-in-simple-python-logging-configuration
tf.get_logger().propagate = False

FLAGS = flags.FLAGS


def model_fn(features, labels, mode, params):
    # training = (mode == tf.estimator.ModeKeys.TRAIN)
    feature_layer = layers.DenseFeatures(params["feature_columns"])
    with tf.GradientTape() as tape:
        input_embeddings = feature_layer(features)
        logging.info("input_embeddings: %d", input_embeddings.get_shape()[1])
        with tf.name_scope("ctr_model"):
            ctr_predictions = DNNModel(units=[512, 256, 128])(input_embeddings)
        with tf.name_scope("cvr_model"):
            cvr_predictions = DNNModel(units=[512, 256, 128])(input_embeddings)

        prop = tf.multiply(ctr_predictions, cvr_predictions, name="CTCVR")

        if mode != tf.estimator.ModeKeys.PREDICT:
            ctr_loss = tf.compat.v1.losses.log_loss(labels['LABEL'], ctr_predictions)
            cvr_loss = tf.compat.v1.losses.log_loss(labels['CONVERSION_LABEL'], prop)
            loss = ctr_loss + cvr_loss

    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'probabilities': tf.identity(prop, name='PREDICTIONS'),  # 也可用ctr_predictions输出
            'ctr_probabilities': ctr_predictions,
            'cvr_probabilities': cvr_predictions
        }

        return tf.estimator.EstimatorSpec(mode, predictions=predictions)

    if mode == tf.estimator.ModeKeys.EVAL:
        metrics = {
            'auc': tf.compat.v1.metrics.auc(labels['LABEL'], ctr_predictions),
            'cvr_auc': tf.compat.v1.metrics.auc(labels['CONVERSION_LABEL'], prop),
            'label/mean': tf.compat.v1.metrics.mean(labels['LABEL']),
            'prediction/mean': tf.compat.v1.metrics.mean(prop)
        }
        return tf.estimator.EstimatorSpec(mode, loss=loss, eval_metric_ops=metrics)

    assert mode == tf.estimator.ModeKeys.TRAIN

    if 'optimizer' not in params:
        learning_rate = tf.compat.v1.train.exponential_decay(FLAGS.learning_rate,
                                                             tf.compat.v1.train.get_global_step(),
                                                             FLAGS.decay_steps,
                                                             FLAGS.decay_coef, staircase=True)
        optimizer = tfa.optimizers.LazyAdam(learning_rate)
        optimizer.iterations = tf.compat.v1.train.get_or_create_global_step()
    else:
        optimizer = params['optimizer']

    trainable_variables = tf.compat.v1.trainable_variables()
    stat_model_parameters(trainable_variables)

    gradients = tape.gradient(loss, trainable_variables)
    optimize = optimizer.apply_gradients(zip(gradients, trainable_variables))

    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=optimize)
