package com.hellozjf.test.hbaseclient;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@Slf4j
public class HbaseClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(HbaseClientApplication.class, args);
    }

    @Autowired
    private Connection connection;

    @PostConstruct
    public void postConstruct() throws IOException {
//        createTable("oss_file_2", Arrays.asList("file", "owner", "content"));
//        listTable();
//        listColumeFamily("oss_file_3");
//        isTableExisted("oss_file_2");
//        isTableDisabled("oss_file_3");
//        disableTable("oss_file_3");
        deleteTable("oss_file_2");
//        listData("oss_file_2");
    }

    /**
     * 列出所有表
     * @throws IOException
     */
    private void listTable() throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            String tableName = hTableDescriptor.getTableName().getNameAsString();
            log.debug("{}", tableName);
        }
    }

    /**
     * 列出表中所有列族
     * @param wantedTableName
     * @throws IOException
     */
    private void listColumeFamily(String wantedTableName) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            String tableName = hTableDescriptor.getTableName().getNameAsString();
            if (tableName.equalsIgnoreCase(wantedTableName)) {
                HColumnDescriptor[] hColumnDescriptors = hTableDescriptor.getColumnFamilies();
                for (HColumnDescriptor hColumnFamilyDescriptor : hColumnDescriptors) {
                    log.debug("{}", hColumnFamilyDescriptor.getNameAsString());
                }
            }
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param columeFamilyNameList
     * @throws IOException
     */
    private void createTable(String tableName, List<String> columeFamilyNameList) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for (String columeFamilyName : columeFamilyNameList) {
            HColumnDescriptor descriptor = new HColumnDescriptor(columeFamilyName);
            table.addFamily(descriptor);
        }
        admin.createTable(table);
    }

    /**
     * 判断table是否存在
     * @param tableName
     * @throws IOException
     */
    private void isTableExisted(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean bExist = admin.tableExists(TableName.valueOf(tableName));
        log.debug("bExist = {}", bExist);
    }

    /**
     * 判断table是否被禁用
     * @param tableName
     * @throws IOException
     */
    private boolean isTableDisabled(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean bDisabled = admin.isTableDisabled(TableName.valueOf(tableName));
        log.debug("bDisabled = {}", bDisabled);
        return bDisabled;
    }

    /**
     * 禁用table
     * @param tableName
     * @throws IOException
     */
    private void disableTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    private void deleteTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (! isTableDisabled(tableName)) {
            disableTable(tableName);
        }
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 查询所有数据
     * @param tableName
     * @throws IOException
     */
    private void listData(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("owner"));
        scan.addFamily(Bytes.toBytes("file"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            log.debug("{}", Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                log.debug("{}", Bytes.toString(cell.getValueArray()));
            }
            break;
        }
    }
}
