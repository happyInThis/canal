package com.alibaba.otter.canal.client.adapter.es.service;

import java.util.*;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * ES 同步 Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
@Component
public class ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncService.class);

    private ESTemplate    esTemplate;

    @Value("${canal.conf.delayTime}")
    private int delayTime = 1000;

    public ESSyncService(ESTemplate esTemplate){
        this.esTemplate = esTemplate;
    }

    public void sync(Collection<ESSyncConfig> esSyncConfigs, Dml dml) {
        long begin = System.currentTimeMillis();
        if (esSyncConfigs != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Destination: {}, database:{}, table:{}, type:{}, affected index count: {}",
                    dml.getDestination(),
                    dml.getDatabase(),
                    dml.getTable(),
                    dml.getType(),
                    esSyncConfigs.size());
            }

            for (ESSyncConfig config : esSyncConfigs) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Prepared to sync index: {}, destination: {}",
                        config.getEsMapping().get_index(),
                        dml.getDestination());
                }
                this.sync(config, dml);
                if (logger.isTraceEnabled()) {
                    logger.trace("Sync completed: {}, destination: {}",
                        config.getEsMapping().get_index(),
                        dml.getDestination());
                }
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms, affected indexes count：{}, destination: {}",
                    (System.currentTimeMillis() - begin),
                    esSyncConfigs.size(),
                    dml.getDestination());
            }
            if (logger.isTraceEnabled()) {
                StringBuilder configIndexes = new StringBuilder();
                esSyncConfigs
                    .forEach(esSyncConfig -> configIndexes.append(esSyncConfig.getEsMapping().get_index()).append(" "));
                logger.trace("DML: {} \nAffected indexes: {}",
                    JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue),
                    configIndexes.toString());
            }
        }
    }

    public void sync(ESSyncConfig config, Dml dml) {
        try {
            // 如果是按时间戳定时更新则返回
            if (config.getEsMapping().isSyncByTimestamp()) {
                return;
            }

            long begin = System.currentTimeMillis();

            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(config, dml);
            } else {
                return;
            }
            long delay = (System.currentTimeMillis() - dml.getEs());
            if (logger.isDebugEnabled()) {
                logger.debug("Sync elapsed time: {} ms , delay: {} ms,destination: {}, dml: {}, es index: {}",
                    (System.currentTimeMillis() - begin),
                    delay,
                    dml.getDestination(),
                    JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue),
                    config.getEsMapping().get_index());
            }
            if("online".equals(config.getEnv()) && delay > config.getDelayTime()) {

                Object id = esTemplate.getValFromData(config.getEsMapping(), dml.getData().get(0), "id", "id");
                DateTime dateTime = new DateTime(System.currentTimeMillis());
                Util.sendWarnMsg("产生时间:" + dateTime.toString("yyyy-MM-dd HH:mm:dd") +
                        "\n索引：" + config.getEsMapping().get_index() +
                        "\n延迟过大：" + delay +
                        "ms\ntable:" + dml.getTable() +
                        "\ntype:" + dml.getType() +
                        "\nid:" + id);
            }
        } catch (Throwable e) {
            logger.error("sync error, es index: {}, DML : {}", config.getEsMapping().get_index(), dml);
            if("online".equals(config.getEnv())) {
                Util.sendWarnMsg("同步失败：" + JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void insert(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                singleTableSimpleFiledInsert(config, dml, data);
            } else {
                // ------是主表 查询sql来插入------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    mainTableInsert(config, dml, data);
                }

                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }
                    // 关联条件出现在主表查询条件是否为简单字段
                    boolean allFieldsSimple = true;
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                            allFieldsSimple = false;
                            break;
                        }
                    }
                    // 所有查询字段均为简单字段
                    if (allFieldsSimple) {
                        // 不是子查询
                        if (!tableItem.isSubQuery()) {
                            // ------关联表简单字段插入------
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                Object value = esTemplate.getValFromData(config.getEsMapping(),
                                    data,
                                    fieldItem.getFieldName(),
                                    fieldItem.getColumn().getColumnName());
                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                            }

                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                        } else {
                            // ------关联子表简单字段插入------
                            subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                        }
                    } else {
                        // ------关联子表复杂字段插入 执行全sql更新es------
                        wholeSqlOperation(config, dml, data, null, tableItem);
                    }
                }
            }
        }
    }

    /**
     * 更新操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void update(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        int i = 0;
        for (Map<String, Object> data : dataList) {
            Map<String, Object> old = oldList.get(i);
            if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                singleTableSimpleFiledUpdate(config, dml, data, old);
            } else {
                // ------主表 查询sql来更新------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    ESMapping mapping = config.getEsMapping();
                    String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
                    FieldItem idFieldItem = schemaItem.getSelectFields().get(idFieldName);

                    boolean idFieldSimple = true;
                    if (idFieldItem.isMethod() || idFieldItem.isBinaryOp()) {
                        idFieldSimple = false;
                    }

                    boolean allUpdateFieldSimple = true;
                    out: for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                        for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                            if (old.containsKey(columnItem.getColumnName())) {
                                if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                                    allUpdateFieldSimple = false;
                                    break out;
                                }
                            }
                        }
                    }

                    // 不支持主键更新!!

                    // 判断是否有外键更新
                    boolean fkChanged = false;
                    for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                        if (tableItem.isMain()) {
                            continue;
                        }
                        boolean changed = false;
                        for (List<FieldItem> fieldItems : tableItem.getRelationTableFields().values()) {
                            for (FieldItem fieldItem : fieldItems) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    fkChanged = true;
                                    changed = true;
                                    break;
                                }
                            }
                        }
                        // 如果外键有修改,则更新所对应该表的所有查询条件数据
                        if (changed) {
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                fieldItem.getColumnItems()
                                    .forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                            }
                        }
                    }

                    // 判断主键和所更新的字段是否全为简单字段
                    if (idFieldSimple && allUpdateFieldSimple && !fkChanged) {
                        singleTableSimpleFiledUpdate(config, dml, data, old);
                    } else {
                        mainTableUpdate(config, dml, data, old);
                    }
                }

                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }

                    // 关联条件出现在主表查询条件是否为简单字段
                    boolean allFieldsSimple = true;
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                            allFieldsSimple = false;
                            break;
                        }
                    }

                    // 所有查询字段均为简单字段
                    if (allFieldsSimple) {
                        // 不是子查询
                        if (!tableItem.isSubQuery()) {
                            // ------关联表简单字段更新------
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    Object value = esTemplate.getValFromData(config.getEsMapping(),
                                        data,
                                        fieldItem.getFieldName(),
                                        fieldItem.getColumn().getColumnName());
                                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                                }
                            }
                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                        } else {
                            // ------关联子表简单字段更新------
                            subTableSimpleFieldOperation(config, dml, data, old, tableItem);
                        }
                    } else {
                        // ------关联子表复杂字段更新 执行全sql更新es------
                        wholeSqlOperation(config, dml, data, old, tableItem);
                    }
                }
            }

            i++;
        }
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void delete(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            ESMapping mapping = config.getEsMapping();

            // ------是主表------
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                if (mapping.get_id() != null) {
                    FieldItem idFieldItem = schemaItem.getIdFieldItem(mapping);
                    // 主键为简单字段
                    if (!idFieldItem.isMethod() && !idFieldItem.isBinaryOp()) {
                        Object idVal = esTemplate.getValFromData(mapping,
                            data,
                            idFieldItem.getFieldName(),
                            idFieldItem.getColumn().getColumnName());

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                        }
                        esTemplate.delete(mapping, idVal, null);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        // FIXME 删除时反查sql为空记录, 无法获获取 id field 值
                        mainTableDelete(config, dml, data);
                    }
                } else {
                    FieldItem pkFieldItem = schemaItem.getIdFieldItem(mapping);
                    if (!pkFieldItem.isMethod() && !pkFieldItem.isBinaryOp()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Object pkVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, pk: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                pkVal);
                        }
                        esFieldData.remove(pkFieldItem.getFieldName());
                        esFieldData.keySet().forEach(key -> esFieldData.put(key, null));
                        esTemplate.delete(mapping, pkVal, esFieldData);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        mainTableDelete(config, dml, data);
                    }
                }

            }

            // 从表的操作
            for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.isMain()) {
                    continue;
                }
                if (!tableItem.getTableName().equals(dml.getTable())) {
                    continue;
                }

                // 关联条件出现在主表查询条件是否为简单字段
                boolean allFieldsSimple = true;
                for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                    if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                        allFieldsSimple = false;
                        break;
                    }
                }

                // 所有查询字段均为简单字段
                if (allFieldsSimple) {
                    // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // ------关联表简单字段更新为null------
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), null);
                        }
                        joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                    } else {
                        // ------关联子表简单字段更新------
                        subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                    }
                } else {
                    // ------关联子表复杂字段更新 执行全sql更新es------
                    wholeSqlOperation(config, dml, data, null, tableItem);
                }
            }
        }
    }

    /**
     * 单表简单字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void singleTableSimpleFiledInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

        if (logger.isTraceEnabled()) {
            logger.trace("Single table insert to es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        esTemplate.insert(mapping, idVal, esFieldData);
    }

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void mainTableInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table insert to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, esFieldData);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table insert to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.insert(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    private void mainTableDelete(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table delete es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                Map<String, Object> esFieldData = null;
                if (mapping.getPk() != null) {
                    esFieldData = new LinkedHashMap<>();
                    esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
                    esFieldData.remove(mapping.getPk());
                    for (String key : esFieldData.keySet()) {
                        esFieldData.put(Util.cleanColumn(key), null);
                    }
                }
                while (rs.next()) {
                    Object idVal = esTemplate.getIdValFromRS(mapping, rs);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table delete to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.delete(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 关联表主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void joinTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                               TableItem tableItem, Map<String, Object> esFieldData) {
        ESMapping mapping = config.getEsMapping();

        Map<String, Object> paramsTmp = new LinkedHashMap<>();
        for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
            for (FieldItem fieldItem : entry.getValue()) {
                if (fieldItem.getColumnItems().size() == 1) {
                    Object value = esTemplate.getValFromData(mapping,
                        data,
                        fieldItem.getFieldName(),
                        entry.getKey().getColumn().getColumnName());

                    String fieldName = fieldItem.getFieldName();
                    // 判断是否是主键
                    if (fieldName.equals(mapping.get_id())) {
                        fieldName = "_id";
                    }
                    paramsTmp.put(fieldName, value);
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index());
        }
        esTemplate.updateByQuery(config, paramsTmp, esFieldData);
    }

    /**
     * 关联子查询, 主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param old 单行old数据
     * @param tableItem 当前表配置
     */
    private void subTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old, TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        StringBuilder sql = new StringBuilder(
            "SELECT * FROM (" + tableItem.getSubQuerySql() + ") " + tableItem.getAlias() + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            ESSyncUtil.appendCondition(sql, value, tableItem.getAlias(), columnName);
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    if (StringUtils.isNotEmpty(config.getEsMapping().getRouting())) {
                        FieldItem routingFieldItem = tableItem.getSchemaItem().getSelectFields().get(config.getEsMapping().getRouting());
                        Object routingVal;
                        routingVal = esTemplate.getValFromData(config.getEsMapping(),
                                data,
                                routingFieldItem.getFieldName(),
                                routingFieldItem.getFieldName());
                        if (routingVal != null) {
                            esFieldData.put("$routing", routingVal.toString());
                        }
                    }
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            out: for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.getFieldName().equals(columnItem0.getColumnName()))
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping,
                                                    rs,
                                                    fieldItem.getFieldName(),
                                                    fieldItem.getColumn().getColumnName());
                                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                                break out;
                                            }
                                        }
                                }
                            }
                        } else {
                            Object val = esTemplate.getValFromRS(mapping,
                                rs,
                                fieldItem.getFieldName(),
                                fieldItem.getColumn().getColumnName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            if (fieldItem.getColumnItems().size() == 1) {
                                Object value = esTemplate.getValFromRS(mapping,
                                    rs,
                                    fieldItem.getFieldName(),
                                    entry.getKey().getColumn().getColumnName());
                                String fieldName = fieldItem.getFieldName();
                                // 判断是否是主键
                                if (fieldName.equals(mapping.get_id())) {
                                    fieldName = "_id";
                                }
                                paramsTmp.put(fieldName, value);
                            }
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 关联(子查询), 主表复杂字段operation, 全sql执行
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void wholeSqlOperation(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old,
                                   TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        //防止最后出现groupby 导致sql解析异常
        String[] sqlSplit = mapping.getSql().split("GROUP\\ BY(?!(.*)ON)");
        String sqlNoWhere = sqlSplit[0];

        String sqlGroupBy = "";

        if(sqlSplit.length > 1){
            sqlGroupBy =  "GROUP BY "+ sqlSplit[1];
        }

        StringBuilder sql = new StringBuilder(sqlNoWhere + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            ESSyncUtil.appendCondition(sql, value, tableItem.getAlias(), columnName);
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        sql.append(sqlGroupBy);

        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query whole sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            // 从表子查询
                            out: for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.getFieldName().equals(columnItem0.getColumnName()))
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping,
                                                    rs,
                                                    fieldItem.getFieldName(),
                                                    fieldItem.getFieldName());
                                                esFieldData.put(fieldItem.getFieldName(), val);
                                                break out;
                                            }
                                        }
                                }
                            }
                            // 从表非子查询
                            for (FieldItem fieldItem1 : tableItem.getRelationSelectFieldItems()) {
                                if (fieldItem1.equals(fieldItem)) {
                                    for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                        if (old.containsKey(columnItem.getColumnName())) {
                                            Object val = esTemplate.getValFromRS(mapping,
                                                rs,
                                                fieldItem.getFieldName(),
                                                fieldItem.getFieldName());
                                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            Object val = esTemplate
                                .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            Object value = esTemplate
                                .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                            String fieldName = fieldItem.getFieldName();
                            // 判断是否是主键
                            if (fieldName.equals(mapping.get_id())) {
                                fieldName = "_id";
                            }
                            paramsTmp.put(fieldName, value);
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace(
                            "Join table update es index by query whole sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 单表简单字段update
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行data数据
     * @param old 单行old数据
     */
    private void singleTableSimpleFiledUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();

        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, old, dml.getTable(), esFieldData);

        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        esTemplate.update(mapping, idVal, esFieldData);
    }

    /**
     * 主表(单表)复杂字段update
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void mainTableUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, old, esFieldData);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table update to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.update(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 提交批次
     */
    public void commit() {
        esTemplate.commit();
    }
}
