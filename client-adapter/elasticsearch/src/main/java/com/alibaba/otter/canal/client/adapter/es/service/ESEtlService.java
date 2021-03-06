package com.alibaba.otter.canal.client.adapter.es.service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import javax.sql.DataSource;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection.ESIndexRequest;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection.ESSearchRequest;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection.ESUpdateRequest;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService extends AbstractEtlService {

    private ESConnection esConnection;
    private ESTemplate   esTemplate;
    private ESSyncConfig config;

    public ESEtlService(ESConnection esConnection, ESSyncConfig config){
        super("ES", config);
        this.esConnection = esConnection;
        this.esTemplate = new ESTemplate(esConnection);
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        ESMapping mapping = config.getEsMapping();
        logger.info("start etl to import data to index: {}", mapping.get_index());
        String sql = mapping.getSql();
        return importData(sql, params);
    }

    private void processFailBulkResponse(BulkResponse bulkResponse) {
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (!response.isFailed()) {
                continue;
            }

            if (response.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                logger.warn(response.getFailureMessage());
            } else {
                errorLogger.error("全量导入数据有误 {},response:{}", response.getFailureMessage(), response);
                throw new RuntimeException("全量数据 etl 异常: " + response.getFailureMessage());
            }
        }
    }

    protected Object executeSqlImport(DataSource ds, String sql, List<Object> values,
            AdapterConfig config, LongAdder impCount,
                                       List<String> errMsg) {
        ESSyncConfig esSyncConfig = (ESSyncConfig) config;
        ESMapping mapping = esSyncConfig.getEsMapping();
        return Util.sqlRS(ds, sql, values, rs -> {
            Long lastId = null;
            int count = 0;
            try {
                ESBulkRequest esBulkRequest = this.esConnection.new ESBulkRequest();
                long batchBegin = System.currentTimeMillis();
                while (rs.next()) {
                    lastId = rs.getLong("id");
                    count += 1;
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = null;
                    Object routingVal = null;
                    for (FieldItem fieldItem : mapping.getSchemaItem().getSelectFields().values()) {

                        String fieldName = fieldItem.getFieldName();
                        if (mapping.getSkips().contains(fieldName)) {
                            continue;
                        }

                        // 如果是主键字段则不插入
                        if (fieldItem.getFieldName().equals(mapping.get_id())) {
                            try {
                                idVal = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                            } catch(Exception e) {
                                errorLogger.error("sync error id参数错误, es index: {}, table: {}, id : {}", mapping.get_index(), esSyncConfig.getDestination(), rs.getObject(fieldName));
                                break;
                            }
                        } else if(fieldItem.getFieldName().equals(mapping.getRouting())) {
                            try {
                                routingVal = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                                esFieldData.put(Util.cleanColumn(fieldName), routingVal);
                            }  catch (SQLException e) {
                                throw new RuntimeException(e);
                            } catch(Exception e) {
                                errorLogger.error("sync error routing参数错误, es index: {}, table: {}, routing : {}", mapping.get_index(), esSyncConfig.getDestination(), rs.getObject(fieldName));
                                break;
                            }
                        } else {
                            Object val = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                            esFieldData.put(Util.cleanColumn(fieldName), val);
                        }
                    }
                    if(idVal == null || routingVal == null) {
                        errorLogger.error("sync error 参数丢失, es index: {}, table: {}, id : {}, pkId:{}, routing:{},data:{}",
                                mapping.get_index(), esSyncConfig.getDestination(), lastId, idVal, routingVal, JSON.toJSONString(esFieldData));
                        continue;
                    }
                    if(StringUtils.isBlank(idVal.toString()) || StringUtils.isBlank(routingVal.toString())) {
                        errorLogger.error("sync error 参数丢失, es index: {}, table: {}, id : {}, pkId:{}, routing:{},data:{}",
                                mapping.get_index(), esSyncConfig.getDestination(), lastId, idVal, routingVal, JSON.toJSONString(esFieldData));
                        continue;
                    }
                    if (!mapping.getRelations().isEmpty()) {
                        mapping.getRelations().forEach((relationField, relationMapping) -> {
                            Map<String, Object> relations = new HashMap<>();
                            relations.put("name", relationMapping.getName());
                            if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                                FieldItem parentFieldItem = mapping.getSchemaItem()
                                    .getSelectFields()
                                    .get(relationMapping.getParent());
                                Object parentVal;
                                try {
                                    parentVal = esTemplate.getValFromRS(mapping,
                                        rs,
                                        parentFieldItem.getFieldName(),
                                        parentFieldItem.getFieldName());
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                                if (parentVal != null) {
                                    relations.put("parent", parentVal.toString());
                                    esFieldData.put("$parent_routing", parentVal.toString());

                                }
                            }

                            esFieldData.put(Util.cleanColumn(relationField), relations);
                        });
                    }

                    if (idVal != null) {
                        String parentVal = (String) esFieldData.remove("$parent_routing");
                        if (mapping.isUpsert()) {
                            ESUpdateRequest esUpdateRequest = this.esConnection.new ESUpdateRequest(
                                mapping.get_index(),
                                mapping.get_type(),
                                idVal.toString()).setDoc(esFieldData).setDocAsUpsert(true);

                            if (routingVal != null) {
                                esUpdateRequest.setRouting(routingVal.toString());
                            }
                            if (StringUtils.isNotEmpty(parentVal)) {
                                esUpdateRequest.setRouting(parentVal);
                            }

                            esBulkRequest.add(esUpdateRequest);
                        } else {
                            ESIndexRequest esIndexRequest = this.esConnection.new ESIndexRequest(mapping
                                .get_index(), mapping.get_type(), idVal.toString()).setSource(esFieldData);
                            if (routingVal != null) {
                                esIndexRequest.setRouting(routingVal.toString());
                            }
                            if (StringUtils.isNotEmpty(parentVal)) {
                                esIndexRequest.setRouting(parentVal);
                            }
                            esBulkRequest.add(esIndexRequest);
                        }
                    } else {
                        idVal = esFieldData.get(mapping.getPk());
                        ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(mapping.get_index(),
                            mapping.get_type()).setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal))
                                .size(10000);
                        SearchResponse response = esSearchRequest.getResponse();
                        for (SearchHit hit : response.getHits()) {
                            ESUpdateRequest esUpdateRequest = this.esConnection.new ESUpdateRequest(mapping
                                .get_index(), mapping.get_type(), hit.getId()).setDoc(esFieldData);
                            esBulkRequest.add(esUpdateRequest);
                        }
                    }

                    if (esBulkRequest.numberOfActions() % mapping.getCommitBatch() == 0
                        && esBulkRequest.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        BulkResponse rp = esBulkRequest.bulk();
                        if (rp.hasFailures()) {
                            this.processFailBulkResponse(rp);
                        }

                        logger.info("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index: {}, firstId:{}, lastId:{}",
                            (System.currentTimeMillis() - batchBegin),
                            (System.currentTimeMillis() - esBatchBegin),
                            esBulkRequest.numberOfActions(),
                            mapping.get_index(),
                            values.get(0),
                            lastId);
                        batchBegin = System.currentTimeMillis();
                        esBulkRequest.resetBulk();
                    }
                    impCount.increment();
                }

                if (esBulkRequest.numberOfActions() > 0) {
                    long esBatchBegin = System.currentTimeMillis();
                    BulkResponse rp = esBulkRequest.bulk();
                    if (rp.hasFailures()) {
                        this.processFailBulkResponse(rp);
                    }
                    logger.info("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}, firstId:{}, lastId:{}",
                        (System.currentTimeMillis() - batchBegin),
                        (System.currentTimeMillis() - esBatchBegin),
                        esBulkRequest.numberOfActions(),
                        mapping.get_index(),
                        values.get(0),
                        lastId);
                }
            } catch (Exception e) {
                errorLogger.error(e.getMessage(), e);
                errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                throw new RuntimeException(e);
            }
            Map result = new HashMap();
            result.put("count", count);
            return result;
        });
    }
}
