import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow_addons as tfa
from rank.losses import alpha_weights
from rank.util import stat_model_parameters
from absl import flags, logging
import collections

tf.get_logger().propagate = False

FLAGS = flags.FLAGS


def _check_fm_columns(feature_columns):
    if isinstance(feature_columns, collections.Iterator):
        feature_columns = list(feature_columns)
    column_num = len(feature_columns)
    if column_num < 2:
        raise ValueError('feature_columns must have as least two elements.')
    dimension = -1
    for column in feature_columns:
        if dimension != -1 and column.dimension != dimension:
            raise ValueError('fm_feature_columns must have the same dimension.')
        dimension = column.dimension
    return column_num, dimension


def model_fn(features, labels, mode, params):
    # training = (mode == tf.estimator.ModeKeys.TRAIN)
    with tf.GradientTape() as tape:
        # 权宜的实现: number_column只参与deep，不参与FM!
        embedding_columns = []
        number_columns = []
        for column in params['feature_columns']:
            # 如何引用EmbeddingColumn？
            # 希望 isinstance(column, EmbeddingColumn)
            if hasattr(column, "dimension"):  # EmbeddingColumn
                embedding_columns.append(column)
            else:  # NumericColumn
                number_columns.append(column)

        input_embeddings = layers.DenseFeatures(embedding_columns)(features)
        logging.info("input_embeddings: %d", input_embeddings.get_shape()[1])

        column_num, dimension = _check_fm_columns(embedding_columns)
        feature_embeddings = tf.reshape(input_embeddings, (-1, column_num, dimension))  # (B, F, K)

        # sum_square part
        summed_feature_embeddings = tf.reduce_sum(feature_embeddings, 1)  # (B, K)
        summed_square_feature_embeddings = tf.square(summed_feature_embeddings)

        # square-sum part
        squared_feature_embeddings = tf.square(feature_embeddings)
        squared_sum_feature_embeddings = tf.reduce_sum(squared_feature_embeddings, 1)

        fm_second_order = 0.5 * tf.subtract(summed_square_feature_embeddings, squared_sum_feature_embeddings)

        deep = tf.keras.models.Sequential([
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu"),
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu"),
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu")
        ])

        # number column直接拼接在后面
        if len(number_columns) == 0:
            deep_input = input_embeddings
        else:
            number_input = layers.DenseFeatures(number_columns)(features)
            logging.info("number_input: %d", number_input.get_shape()[1])
            deep_input = tf.concat([input_embeddings, number_input], 1)
            logging.info("deep_input: %d", deep_input.get_shape()[1])

        last_deep_layer = deep(deep_input)
        last_layer = tf.concat([fm_second_order, last_deep_layer], axis=1)
        logits = layers.Dense(units=1, activation=keras.activations.sigmoid)(last_layer)

        if mode != tf.estimator.ModeKeys.PREDICT:
            loss = tf.compat.v1.losses.log_loss(labels, logits)

    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'PRED': tf.identity(logits, name='PREDICTIONS')
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
        'auc': tf.compat.v1.metrics.auc(labels, logits),
        'label/mean': tf.compat.v1.metrics.mean(labels),
        'prediction/mean': tf.compat.v1.metrics.mean(logits)
    }

    if mode == tf.estimator.ModeKeys.EVAL:
        return tf.estimator.EstimatorSpec(mode, loss=loss, eval_metric_ops=metrics)

    trainable_variables = tf.compat.v1.trainable_variables()
    stat_model_parameters(trainable_variables)

    gradients = tape.gradient(loss, trainable_variables)
    optimize = optimizer.apply_gradients(zip(gradients, trainable_variables))
    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=optimize)


def model_opt_fn(features, labels, mode, params):
    # training = (mode == tf.estimator.ModeKeys.TRAIN)
    with tf.GradientTape() as tape:

        categorical_columns = []
        continuous_columns = []

        for column in params['feature_columns']:
            # 如何引用EmbeddingColumn？
            # 希望 isinstance(column, EmbeddingColumn)
            if hasattr(column, "dimension"):  # EmbeddingColumn
                categorical_columns.append(column)
            else:  # NumericColumn
                continuous_columns.append(column)

        # categorical features
        categorical_input_embeddings = layers.DenseFeatures(categorical_columns)(features)  # (B, categorical_num * K)
        categorical_num, dimension = _check_fm_columns(categorical_columns)

        # continuous features
        continuous_values = layers.DenseFeatures(continuous_columns)(features)  # (B, continuous_num)
        continuous_num = continuous_values.get_shape()[1]

        input_embeddings = tf.concat([categorical_input_embeddings, continuous_values], 1)  # (B, categorical_num * K + continuous_num)

        logging.info("categorical_num: %d", categorical_num)
        logging.info("continuous_num: %d", continuous_num)
        logging.info("input_embeddings: %s", input_embeddings.get_shape())

        categorical_embeddings = tf.reshape(categorical_input_embeddings, (-1, categorical_num, dimension))  # (B, categorical_num, K)

        # create latent vectors for the continuous features
        continuous_latent_vectors = tf.Variable(initial_value=tf.random.normal([continuous_num, dimension]),
                                                name="continuous_latent_vectors")  # (continuous_num, K)

        continuous_embeddings = tf.tile(tf.expand_dims(continuous_latent_vectors, 0),
                                        [FLAGS.batch_size, 1, 1])  # (B, continuous_num, K)
        continuous_embeddings = continuous_embeddings * tf.reshape(continuous_values, (-1, continuous_num, 1))  # (B, continuous_num, K)

        feature_embeddings = tf.concat([categorical_embeddings, continuous_embeddings], axis=1)  # (B, F, K)
        logging.info("feature_embeddings: %s", feature_embeddings.get_shape())

        # sum_square part
        summed_feature_embeddings = tf.reduce_sum(feature_embeddings, 1)  # (B, K)
        summed_square_feature_embeddings = tf.square(summed_feature_embeddings)

        # square-sum part
        squared_feature_embeddings = tf.square(feature_embeddings)
        squared_sum_feature_embeddings = tf.reduce_sum(squared_feature_embeddings, 1)

        fm_second_order = 0.5 * tf.subtract(summed_square_feature_embeddings, squared_sum_feature_embeddings)

        deep = tf.keras.models.Sequential([
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu"),
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu"),
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu")
        ])

        last_deep_layer = deep(input_embeddings)
        last_layer = tf.concat([fm_second_order, last_deep_layer], axis=1)
        logits = layers.Dense(units=1, activation=keras.activations.sigmoid)(last_layer)

        if mode != tf.estimator.ModeKeys.PREDICT:
            loss = tf.compat.v1.losses.log_loss(labels, logits)

    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'PRED': tf.identity(logits, name='PREDICTIONS')
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
        'auc': tf.compat.v1.metrics.auc(labels, logits),
        'label/mean': tf.compat.v1.metrics.mean(labels),
        'prediction/mean': tf.compat.v1.metrics.mean(logits)
    }

    if mode == tf.estimator.ModeKeys.EVAL:
        return tf.estimator.EstimatorSpec(mode, loss=loss, eval_metric_ops=metrics)

    trainable_variables = tf.compat.v1.trainable_variables()
    stat_model_parameters(trainable_variables)

    gradients = tape.gradient(loss, trainable_variables)
    optimize = optimizer.apply_gradients(zip(gradients, trainable_variables))
    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=optimize)
