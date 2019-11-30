package com.alibaba.otter.canal.client.adapter.es.test;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;

public class SqlParseTest {

    @Test
    public void parseTest() {
//        String sql = "select a.id, CASE WHEN a.id <= 500 THEN '1' else '2' end as id2, "
//                     + "concat(a.name,'_test') as name, a.role_id, b.name as role_name, c.labels from user a "
//                     + "left join role b on a.role_id=b.id "
//                     + "left join (select user_id, group_concat(label,',') as labels from user_label "
//                     + "group by user_id) c on c.user_id=a.id";
        String sql = "select a.id as id,\n" + "a.item_id as item_id,\n" + "a.item_name as item_name,\n" + "a.item_image_url as item_image_url,\n" + "a.item_sku_desc as item_sku_desc,\n" + "a.unit_price as unit_price,\n" + "a.delivery_mark as delivery_mark,\n" + "a.gmt_modified as item_gmt_modified,\n" + "a.cost_price as cost_price,\n" + "a.delivery_fee as item_delivery_fee,\n" + "a.item_type as item_type,\n" + "a.seller_id as seller_id,\n" + "a.number as number,\n" + "a.bar_code as bar_code,\n" + "a.id as item_primary_id,\n" + "a.service_fee as service_fee,\n" + "a.star_bonus_fee as star_bonus_fee,\n" + "a.discount_amount as item_discount_amount,\n" + "a.delivery_info_id as delivery_info_id,\n" + "a.refund_type as refund_type,\n" + "a.refund_status as refund_status,\n" + "a.item_sku_id as item_sku_id,\n" + "a.refund_time as refund_time\n" + " from placeholder a";
        SchemaItem schemaItem = SqlParser.parse(sql);

        // 通过表名找 TableItem
        List<TableItem> tableItems = schemaItem.getTableItemAliases().get("placeholder".toLowerCase());
        tableItems.forEach(tableItem -> Assert.assertEquals("a", tableItem.getAlias()));

        TableItem tableItem = tableItems.get(0);
//        Assert.assertFalse(tableItem.isMain());
//        Assert.assertTrue(tableItem.isSubQuery());
        // 通过字段名找 FieldItem
        List<FieldItem> fieldItems = schemaItem.getColumnFields().get(tableItem.getAlias() + ".item_id".toLowerCase());
        fieldItems.forEach(
            fieldItem -> Assert.assertEquals("a.item_id", fieldItem.getOwner() + "." + fieldItem.getFieldName()));

        // 获取当前表关联条件字段
        Map<FieldItem, List<FieldItem>> relationTableFields = tableItem.getRelationTableFields();
        relationTableFields.keySet()
            .forEach(fieldItem -> Assert.assertEquals("item_id", fieldItem.getColumn().getColumnName()));

        // 获取关联字段在select中的对应字段
        // List<FieldItem> relationSelectFieldItem =
        // tableItem.getRelationKeyFieldItems();
        // relationSelectFieldItem.forEach(fieldItem -> Assert.assertEquals("c.labels",
        // fieldItem.getOwner() + "." + fieldItem.getColumn().getColumnName()));
    }
}
