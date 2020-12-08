#游戏推荐比赛代码

```mc-game-model```
* java(mapreduce)： 生成模型训练所需tfrecord文件、类别特征词表
* python: 模型训练代码，没有添加cloud-ml的配置文件，只可以本地训练
* scala: 协同过滤、数据分析、生成graphsage训练数据、生成user、item、context结构体parquet文件（供```java```module生成tfrecord等）

```mc-game-scores```
* 特征抽取class，install成jar包，供```mc-game-model```调用

```sql```
* 数据分析、hive统计统计特征等

```node2vec_spark```
* spark版node2vec算法实现
* 用作生成item、cate、tag的向量

```graphsage```
* graphsage算法开源实现，我对其做了详细的标注
* 实际使用中使用阿里开源的graph-learn，做bi-graphsage训练，还是后续改成纯tf实现的方便
