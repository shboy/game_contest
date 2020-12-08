import tensorflow as tf

_WEIGHTED_SUFFIX = "_VALUE"


class FeatureSpec:
    def __init__(self, dtype, is_list=False, fixed=True, size=None):
        """
        feature specification
        :param dtype: {"int", "float", "string", "weighted"}
        :param is_list: whether the feature is a list of values or a single value?
        :param fixed: whether the size of values is fixed?
        :param size: feature value size if `fixed` is True
        """
        assert dtype in {"int", "float", "string", "weighted"}
        self.dtype = dtype
        self.is_list = is_list
        self.fixed = fixed
        self.size = size
        self.spec = self._parse_spec()

    def _parse_spec(self):
        if self.dtype == "weighted":
            if self.fixed:
                return (tf.io.FixedLenFeature(shape=(self.size,), dtype=tf.string, default_value="-1"),
                        tf.io.FixedLenFeature(shape=(self.size,), dtype=tf.float32, default_value=0))
            else:
                return tf.io.VarLenFeature(tf.string), tf.io.VarLenFeature(tf.float32)

        if self.dtype == "string":
            spec = tf.io.VarLenFeature(dtype=tf.string)

        else:  # {"float", "int"}:
            dtype_ = tf.float32 if self.dtype == "float" else tf.int64
            if self.is_list and self.fixed:  # 比如200d的句子w2v向量
                assert self.size is not None and self.size > 0
                # spec = tf.io.FixedLenFeature(shape=(self.size,), dtype=dtype_) # FIXME: 数据有误，临时改一下！！！！
                spec = tf.io.FixedLenFeature(shape=(self.size,), dtype=dtype_, default_value=0)
            elif self.is_list and not self.fixed:
                spec = tf.io.VarLenFeature(dtype=dtype_)
            else:  # scalar value:
                # 当为Fixed类型时，default_value不能为默认值None，否则，当某个batch里没有该特征便会报错！(只要有一条样本没有也会报错？)
                spec = tf.io.FixedLenFeature(shape=(1,), dtype=dtype_, default_value=0)
        return spec

    def __str__(self):
        return str(self.spec)


class FeatureBean:
    def __init__(self, name, group, feature_spec, emb_width):
        """
        :param name: feature name
        :param group: ITEM or USER_CONTEXT
        :param feature_spec: an instance of FeatureSpec
        :param emb_width: embedding size, 分桶或one-hot之后映射成emb_width维向量
        """
        self.name = name
        self.group = group
        self.feature_spec = feature_spec
        self.emb_width = emb_width

    def __str__(self):
        return "name: %s, group: %s, type:%s, emb_width:%d" % (
            self.name, self.group, self.feature_spec.dtype, self.emb_width)


def get_feature_table(FEATURE_CONFIGS, feature_names=None):
    """feature_names: a list of feature names
    :return
       {feature_name: FeatureBean()}
    """
    feature_table = dict(
        (config[0], FeatureBean(config[0], config[1], config[2], config[3])) for config in FEATURE_CONFIGS)
    if feature_names is None:
        return feature_table
    all_features = set(feature_table.keys())
    if not set(feature_names).issubset(all_features):
        raise ValueError("unknown features: %s" % (feature_names - all_features))
    return dict((name, feature) for name, feature in feature_table.items() if name in feature_names)


def get_feature_specs(feature_table):
    feature_specs = {}
    for feature_name, feature in feature_table.items():
        if feature.feature_spec.dtype == "weighted":
            assert type(feature.feature_spec.spec) is tuple
            feature_specs[feature_name] = feature.feature_spec.spec[0]
            feature_specs[feature_name + _WEIGHTED_SUFFIX] = feature.feature_spec.spec[1]
        else:
            feature_specs[feature_name] = feature.feature_spec.spec
    return feature_specs
