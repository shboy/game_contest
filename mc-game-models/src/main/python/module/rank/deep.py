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
        emb_input = feature_layer(features)
        logging.info("input_embeddings: %d", emb_input.get_shape()[1])
        dnn_model = DNNModel(units=[512, 256, 128])
        predictions = dnn_model(emb_input)
        if mode != tf.estimator.ModeKeys.PREDICT:
            if FLAGS.alpha_weight == -1:  # not class weighted
                loss_weights = 1
            else:
                loss_weights = alpha_weights(labels, alpha=FLAGS.alpha_weight)
            loss = tf.compat.v1.losses.log_loss(labels, predictions, weights=loss_weights)

    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'PRED': tf.identity(predictions, name='PREDICTIONS')
        }
        return tf.estimator.EstimatorSpec(mode, predictions=predictions)

    if 'optimizer' not in params:
        learning_rate = tf.compat.v1.train.exponential_decay(FLAGS.learning_rate,
                                                             tf.compat.v1.train.get_global_step(),
                                                             FLAGS.decay_steps,
                                                             FLAGS.decay_coef, staircase=True)
        optimizer = tfa.optimizers.LazyAdam(learning_rate)
        optimizer.iterations = tf.compat.v1.train.get_or_create_global_step()
    else:
        optimizer = params['optimizer']

    metrics = {
        'auc': tf.compat.v1.metrics.auc(labels, predictions),
        'label/mean': tf.compat.v1.metrics.mean(labels),
        'prediction/mean': tf.compat.v1.metrics.mean(predictions)
    }

    if mode == tf.estimator.ModeKeys.EVAL:
        return tf.estimator.EstimatorSpec(mode, loss=loss, eval_metric_ops=metrics)

    # summary_hook = tf.estimator.SummarySaverHook(
    #     save_steps=100,
    #     output_dir="d:/tmp",
    #     scaffold=tf.compat.v1.train.Scaffold(summary_op=tf.compat.v1.summary.merge_all())
    # )

    trainable_variables = tf.compat.v1.trainable_variables()
    stat_model_parameters(trainable_variables)

    emb_dict = dict((v.name, tf.reduce_mean(v)) for v in trainable_variables if
                    v.name.find("dense_feature") != -1)
    # hook = tf.estimator.LoggingTensorHook(emb_dict, every_n_iter=10000)

    gradients = tape.gradient(loss, trainable_variables)
    optimize = optimizer.apply_gradients(zip(gradients, trainable_variables))

    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=optimize)
