package com.zjb.app.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase工具类
 *
 * @author Joel
 */
public class HBaseUtil {

    private Configuration configuration = null;
    private Connection connection = null;

    private static HBaseUtil instance = null;

    private HBaseUtil() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://hadoop001:8020/hbase");
        configuration.set("hbase.zookeeper.quorum", "hadoop000");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized HBaseUtil getInstance() {
        if (null == instance) {
            instance = new HBaseUtil();
        }
        return instance;
    }

    public Table getHTable(String tableName) {
        Table table = null;
        TableName tb = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tb);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String table, String rowKey, String familyName, String qualifier, String value) {
        Table tb = getHTable(table);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        try {
            tb.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        try {
//            System.out.println(HBaseUtil.getInstance().getHTable("course_clickcount").getTableDescriptor().getRegionReplication());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        HBaseUtil.getInstance().put("course_clickcount", "20171111_1", "info", "click_count", "1");
    }
}
