package com.alibaba.otter.canal.client.adapter.support;

import java.util.List;

public interface AdapterConfig {
    String getEnv();
    Integer getQueryBatchSize();
    List<String> getTableNameList();
    String getDataSourceKey();

    AdapterMapping getMapping();

    interface AdapterMapping {
        String getEtlCondition();
    }

}
