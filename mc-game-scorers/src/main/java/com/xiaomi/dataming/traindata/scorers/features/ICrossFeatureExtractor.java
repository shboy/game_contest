package com.xiaomi.dataming.traindata.scorers.features;

import org.apache.thrift.TBase;

/*
*  同时涉及user和item的特征，比如：用户对推荐item的历史ctr。
* */
public interface ICrossFeatureExtractor<U extends TBase, I extends TBase, C extends TBase> extends IFeatureExtractor<U, I, C>{

}


