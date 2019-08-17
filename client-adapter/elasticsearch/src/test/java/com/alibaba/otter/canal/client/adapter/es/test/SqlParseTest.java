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
        String sql = "select a.id as _id,\n" + "a.item_id as item_id,\n" + "a.item_name as item_name,\n" + "a.item_image_url as item_image_url,\n" + "a.item_sku_desc as item_sku_desc,\n" + "a.unit_price as unit_price,\n" + "a.cost_price as cost_price,\n" + "a.delivery_fee as item_delivery_fee,\n" + "a.item_type as item_type,\n" + "a.number as number,\n" + "a.bar_code as bar_code,\n" + "a.service_fee as service_fee,\n" + "a.star_bonus_fee as star_bonus_fee,\n" + "a.discount_amount as item_discount_amount,\n" + "a.refund_type as refund_type,\n" + "a.refund_status as refund_status,\n" + "a.refund_time as refund_time,\n" + "a.order_id as item_order_id,\n" + "b.id as order_id,\n" + "b.biz_code as biz_code,\n" + "b.order_sn as order_sn,\n" + "b.type as type,\n" + "b.user_id as user_id,\n" + "b.user_name as user_name,\n" + "b.seller_id as seller_id,\n" + "b.consignee as consignee,\n" + "b.consignee_mobile as consignee_mobile,\n" + "b.order_time as order_time,\n" + "b.total_price as total_price,\n" + "b.delivery_fee as delivery_fee,\n" + "b.discount_amount as discount_amount,\n" + "b.total_amount as total_amount,\n" + "b.star_user_id as star_user_id,\n" + "b.promote_mark as promote_mark,\n" + "b.pay_time as pay_time,\n" + "b.head_img_url as head_img_url,\n" + "b.gmt_created as gmt_created,\n" + "b.gmt_modified as gmt_modified,\n" + "b.order_status as order_status,\n" + "b.delivery_time as delivery_time,\n" + "b.receipt_time as receipt_time,\n" + "b.refund_mark as refund_mark,\n" + "b.settlement_mark as settlement_mark,\n" + "b.rookie_mark as rookie_mark,\n" + "b.show_flag as show_flag,\n" + "b.purchase_status as purchase_status,\n" + "b.seller_memo as seller_memo,\n" + "\n" + "c.province as province,\n" + "c.city as city,\n" + "c.area as area\n" + "from order_item a LEFT JOIN user_order b on a.order_id = b.id LEFT JOIN  order_consignee c on c.order_id = b.id\n" + "where a.id > 1000000 and a.id < 2000000 order by a.id LIMIT 10";
        SchemaItem schemaItem = SqlParser.parse(sql);

        // 通过表名找 TableItem
        List<TableItem> tableItems = schemaItem.getTableItemAliases().get("user_label".toLowerCase());
        tableItems.forEach(tableItem -> Assert.assertEquals("c", tableItem.getAlias()));

        TableItem tableItem = tableItems.get(0);
        Assert.assertFalse(tableItem.isMain());
        Assert.assertTrue(tableItem.isSubQuery());
        // 通过字段名找 FieldItem
        List<FieldItem> fieldItems = schemaItem.getColumnFields().get(tableItem.getAlias() + ".labels".toLowerCase());
        fieldItems.forEach(
            fieldItem -> Assert.assertEquals("c.labels", fieldItem.getOwner() + "." + fieldItem.getFieldName()));

        // 获取当前表关联条件字段
        Map<FieldItem, List<FieldItem>> relationTableFields = tableItem.getRelationTableFields();
        relationTableFields.keySet()
            .forEach(fieldItem -> Assert.assertEquals("user_id", fieldItem.getColumn().getColumnName()));

        // 获取关联字段在select中的对应字段
        // List<FieldItem> relationSelectFieldItem =
        // tableItem.getRelationKeyFieldItems();
        // relationSelectFieldItem.forEach(fieldItem -> Assert.assertEquals("c.labels",
        // fieldItem.getOwner() + "." + fieldItem.getColumn().getColumnName()));
    }
}
