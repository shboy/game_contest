from rank.feature_table import FeatureSpec

"""
qcut
USER_DOMAIN_PV [303068] [(0.999, 11.0] < (11.0, 21.0] < (21.0, 33.0] < (33.0, 47.0] < (47.0, 65.0] < (65.0, 91.0] < (91.0, 140.0] < (140.0, 15327.0]]
USER_TOPIC_ACCEPT_RATE [125783] [(0.0217, 0.167] < (0.167, 0.2] < (0.2, 0.25] < (0.25, 0.333] < (0.333, 0.4] < (0.4, 0.5] < (0.5, 0.667] < (0.667, 1.0]]
DOMAIN_UV [865619] [(15503.999, 164348.0] < (164348.0, 293176.0] < (293176.0, 633404.0] < (633404.0, 914848.0] < (914848.0, 1453470.0] < (1453470.0, 3110576.0] < (3110576.0, 3140199.0] < (3140199.0, 3141358.0]]
TOPIC_PV [865619] [(1.999, 102086.0] < (102086.0, 288734.0] < (288734.0, 526570.0] < (526570.0, 1211900.0] < (1211900.0, 1282462.0] < (1282462.0, 3459278.0] < (3459278.0, 3652605.0] < (3652605.0, 3675494.0]]
TOPIC_ACCEPT_RATE [865619] [(-0.001, 0.139] < (0.139, 0.161] < (0.161, 0.163] < (0.163, 0.164] < (0.164, 0.165] < (0.165, 0.178] < (0.178, 0.191] < (0.191, 0.216]]
USER_TOPIC_PV [] [(0.999, 3.0] < (3.0, 4.0] < (4.0, 5.0] < (5.0, 7.0] < (7.0, 44.0]]
"""

USER_CONTEXT = 'USER_CONTEXT'
ITEM = 'ITEM'

INT = 'int'
FLOAT = 'float'
STRING = 'string'

"""
class FeatureBean
:param name: feature name
:param group: ITEM or USER_CONTEXT
:param feature_spec: an instance of FeatureSpec
:param emb_width: embedding size
"""
FEATURE_CONFIGS = [
    #######################
    # USER_CONTEXT FIXME: 为什么不分开？
    #######################
    # context
    # ('MASTER_DOMAIN', 'USER_CONTEXT', FeatureSpec("string"), 4),
    # ('EXPOSE_TIME', 'USER_CONTEXT', FeatureSpec("string"), 4),

    # user
    ('PHONE_BRAND', USER_CONTEXT, FeatureSpec(STRING), 6),
    ('RESIDENT_CITY', USER_CONTEXT, FeatureSpec(STRING), 6),
    ('USER_AGE', USER_CONTEXT, FeatureSpec(STRING), 3),
    ('USER_DEGREE', USER_CONTEXT, FeatureSpec(STRING), 4),
    ('USER_SEX', USER_CONTEXT, FeatureSpec(STRING), 2),
    # ('RESIDENT_CITY', 'USER_CONTEXT', FeatureSpec("string"), 6),
    # ('RESIDENT_PROVINCE', 'USER_CONTEXT', FeatureSpec("string"), 6),
    # ('PHONE_MODEL', 'USER_CONTEXT', FeatureSpec("string"), 6),
    # ('PHONE_BRAND', 'USER_CONTEXT', FeatureSpec("string"), 6),
    # ('USER_AGE', 'USER_CONTEXT', FeatureSpec("string"), 3),
    # ('USER_SEX', 'USER_CONTEXT', FeatureSpec("string"), 2),

    # USER item level recall score
    ('USER_ITEM_RECALL_SCORE', USER_CONTEXT, FeatureSpec(FLOAT), 6),
    # 30天内，user对slave domain的pv
    #('USER_DOMAIN_PV', 'USER_CONTEXT', FeatureSpec("int"), 6),

    # 30天内，user对topic的pv、accept
    # ('USER_TOPIC_PV', 'USER_CONTEXT', FeatureSpec("int"), 6),
    # ('USER_TOPIC_ACCEPT', 'USER_CONTEXT', FeatureSpec("int"), 6),
    # ('USER_TOPIC_ACCEPT_RATE', 'USER_CONTEXT', FeatureSpec("float"), 6),

    #######################
    # ITEM
    #######################
    ('ITEM_ID', ITEM, FeatureSpec(STRING), 100),
    # ('PAID_RANK_RADIOS', ITEM, FeatureSpec(FLOAT, is_list=True, fixed=True, size=5), None), # TODO: 名字错了应该是 PAID_MAX_RANK_RADIOS

    # ('QUERY_DOMAIN', 'ITEM', FeatureSpec("string"), 3),
    # ('QUERY_TOPIC', 'ITEM', FeatureSpec("string"), 3),
    # ('TOPIC_RULE_ID', 'ITEM', FeatureSpec("string"), 3),

    # 7天内， slave domain UV
    #('DOMAIN_UV', 'ITEM', FeatureSpec("int"), 3),

    # 30天内，TOPIC 曝光、接受、接受率
    # ('TOPIC_PV', 'ITEM', FeatureSpec("int"), 5),
    # ('TOPIC_ACCEPT', 'ITEM', FeatureSpec("int"), 5),
    # ('TOPIC_ACCEPT_RATE', 'ITEM', FeatureSpec("float"), 5),
    # TOPIC_PV: [3454575] TOPIC_ACCEPT_RATE: [0.166] => TOPIC_ACCEPT_RATE_X_PV: ['(0.16, 0.17]|>500000.0']
    # ('TOPIC_ACCEPT_RATE_X_PV', 'USER_CONTEXT', FeatureSpec("string"), 3),
]
