package com.alibaba.otter.canal.client.adapter.support;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractEtlService {

    protected Logger      logger = LoggerFactory.getLogger(this.getClass());

    private String        type;
    private AdapterConfig config;
    private final long CNT_PER_TASK = 10000L;

    public AbstractEtlService(String type, AdapterConfig config){
        this.type = type;
        this.config = config;
    }

    protected EtlResult importData(String sql, List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        if (config == null) {
            logger.warn("{} mapping config is null, etl go end ", type);
            etlResult.setErrorMessage(type + "mapping config is null, etl go end ");
            return etlResult;
        }

        long start = System.currentTimeMillis();
        try {
            DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());


            Long startId = Long.valueOf(params.get(0));
            Long endId = Long.valueOf(params.get(1));
            List<Object> values = new ArrayList<>();
            // 拼接条件
            if (config.getMapping().getEtlCondition() != null && params != null) {
                String etlCondition = config.getMapping().getEtlCondition();
                for (String param : params) {
                    etlCondition = etlCondition.replace("{}", "?");
                    values.add(param);
                }

                sql += " " + etlCondition;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0L : count;
            });

            // 当大于1万条记录时开启多线程
            if (cnt >= 10000) {

                long size = CNT_PER_TASK;
                Map result ;
                int count = 1;
                while(endId > startId && count > 0) {
                    String sqlFinal = sql + " LIMIT " + size;
                    result = (Map) executeSqlImport(dataSource, sqlFinal, values, config.getMapping(), impCount, errMsg);
                    startId = (Long) result.get("lastId");
                    count = (int) result.get("count");
                    values.remove(0);
                    values.add(0, startId);
                }
            } else {
                executeSqlImport(dataSource, sql, values, config.getMapping(), impCount, errMsg);
            }

            logger.info("数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入" + type + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(type + " 数据导入异常 =>" + e.getMessage() + "," + JSON.toJSONString(errMsg));
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }

    protected abstract Object executeSqlImport(DataSource ds, String sql, List<Object> values,
                                                AdapterConfig.AdapterMapping mapping, AtomicLong impCount,
                                                List<String> errMsg);

}
