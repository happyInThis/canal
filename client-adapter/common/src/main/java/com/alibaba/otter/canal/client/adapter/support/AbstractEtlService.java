package com.alibaba.otter.canal.client.adapter.support;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.google.common.base.Joiner;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

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
        LongAdder impCount = new LongAdder();
        List<String> errMsg = new ArrayList<>();
        if (config == null) {
            logger.warn("{} mapping config is null, etl go end ", type);
            etlResult.setErrorMessage(type + "mapping config is null, etl go end ");
            return etlResult;
        }

        long startTime = System.currentTimeMillis();
        try {
            DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());

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
            String startIdSql = sql + " order by id asc limit 1";
            Long startId = (Long) Util.sqlRS(dataSource, startIdSql, values, rs -> {
                Long id = null;
                try {
                    if (rs.next()) {
                        id = rs.getLong("id");
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return id == null ? 0L : id - 1;
            });
            String endIdSql = sql + " order by id desc limit 1";
            Long endId = (Long) Util.sqlRS(dataSource, endIdSql, values, rs -> {
                Long id = null;
                try {
                    if (rs.next()) {
                        id = rs.getLong("id");
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return id == null ? 0L : id;
            });

            int threadCount = Runtime.getRuntime().availableProcessors() * 2;

            long shard = endId / threadCount;
            long size = CNT_PER_TASK;

            String sqlFinal = sql + " order by id asc";
            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sqlFinal);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("shard {} startId {} endId {} threadCount {}", shard, startId, endId, threadCount);
            }
            NamedThreadFactory namedThreadFactory = new NamedThreadFactory("Full-thread-");
            ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L, namedThreadFactory);
            List<Future<Map>> futures = new ArrayList<>();


            for(int i = 1; i <= threadCount; i++) {
                final Long from = startId + shard * (i - 1);
                final Long to;
                if(i == threadCount) {
                    to = endId;
                } else {
                    to = from + shard;
                }
                Future future = executor.submit(
                        () -> {
                            long fromId = from;
                            long toId = fromId + size;
                            List<Object> innerValues = new ArrayList();
                            for(String value : params) {
                                innerValues.add(value);
                            }
                            innerValues.remove(0);
                            innerValues.remove(0);
                            innerValues.add(0, toId);
                            innerValues.add(0, fromId);
                            while(to > fromId) {
                                try {
                                    executeSqlImport(dataSource, sqlFinal, innerValues, config.getMapping(), impCount, errMsg);
                                } catch(Exception e) {
                                    logger.error(String.format("全量数据批量导入 异常 currentThread:%s fromId:%s, toId:%s, msg:%s",
                                            Thread.currentThread().getName(),
                                            fromId,
                                            toId), e);
                                    DateTime dateTime = new DateTime(System.currentTimeMillis());
                                    Util.sendWarnMsg(String.format("time:%s 同步失败 fromId:%d toId:%d",
                                            dateTime.toString("yyyy-MM-dd HH:mm:dd"),
                                            fromId,
                                            toId));
                                    try {
                                        Thread.sleep(500L);
                                    } catch(InterruptedException ex) {

                                    }
                                }
                                fromId = toId;
                                toId = fromId + size;
                                innerValues.remove(0);
                                innerValues.remove(0);
                                innerValues.add(0, toId);
                                innerValues.add(0, fromId);
                            }
                            logger.info("currentThread:{} 全量数据批量导入完成", Thread.currentThread().getName());
                        });
                futures.add(future);
            }

            for (Future future : futures) {
                future.get();
            }
            executor.shutdown();

            logger.info("数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.longValue(), System.currentTimeMillis() - startTime);
            etlResult.setResultMessage("导入" + type + " 数据：" + impCount.longValue() + " 条");
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
                                                AdapterConfig.AdapterMapping mapping, LongAdder impCount,
                                                List<String> errMsg);

}
