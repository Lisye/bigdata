package com.leo.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseApiTest {

    public static Configuration conf;

    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.16.185.102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void main(String[] args) throws IOException {
//        boolean isTableExist = isTableExist("student");
//        System.out.println("isTableExist = " + isTableExist);
//
//        createTable("teach", "info");

//        dropTable("emp");
//        dropTable("teach");

//        addRow("student", "1002", "info", "name", "client");

//        delMultiRow("student", "1002", "1003");

//        getAllRows("student");

//        getRow("student", "1001");

        getRowQualifier("student", "1001", "info", "name");
        getRowQualifier("student", "1001", "info", "age");
    }

    public static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isTableExist(tableName)) {
            System.out.println("table " + tableName + " already existed");
            return;
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            admin.createTable(descriptor);
            System.out.println("create table " + tableName + " successful");
        }
    }

    public static void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isTableExist(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));

            System.out.println("drop table " + tableName + " successful");
        } else {
            System.out.println("table " + tableName + " not exists");
        }
    }

    public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        if (isTableExist(tableName)) {
            System.out.println("table " + tableName + " not exists");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(conf);
        HTable htable = (HTable) connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        htable.put(put);
        htable.close();

        System.out.println("add row successs");
    }

    public static void delMultiRow(String tableName, String... rows) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));

        List<Delete> deletes = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }
        hTable.delete(deletes);
        hTable.close();

        System.out.println("delete tables successful");
    }

    public static void getAllRows(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);

        for (Result result: scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("rowKey = " + rowKey);
                System.out.println("family = " + family);
                System.out.println("qualifier = " + qualifier);
                System.out.println("value = " + value);
                System.out.println();
            }
        }

        hTable.close();
    }

    public static void getRow(String tableName, String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println("rowKey = " + rowKey);
            System.out.println("family = " + family);
            System.out.println("qualifier = " + qualifier);
            System.out.println("value = " + value);
            System.out.println();
        }

        hTable.close();
    }

    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
//            rowKey = Bytes.toString(CellUtil.cloneRow(cell));
//            family = Bytes.toString(CellUtil.cloneFamily(cell));
            qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println("rowKey = " + rowKey);
            System.out.println("family = " + family);
            System.out.println("qualifier = " + qualifier);
            System.out.println("value = " + value);
            System.out.println();
        }

        hTable.close();
    }
}
