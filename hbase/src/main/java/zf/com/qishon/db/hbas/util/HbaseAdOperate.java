package zf.com.qishon.db.hbas.util;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
admin 操作类
*/
public class HbaseAdOperate {
    // 日志类
    private static final Logger logger = LoggerFactory.getLogger(HbaseAdOperate.class);
    //获取连接
    public static Connection connection;
    //获取admin权限
    public static Admin admin;
    //连接初始化
    static {
        try {
            connection = HbaseConnection.getConnection();
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建命名空间？？

    // 创建表
    public static void createTable(String tableName, String[] colFamily, int maxVersion) throws IOException {

        // 获取hbase table对象
        TableName hTableName = TableName.valueOf(tableName);

        try {
            if (!admin.tableExists(hTableName)) {
                // 生成表描述
                HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);
                // 循环添加列族
                for (String col : colFamily) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                    hColumnDescriptor.setMaxVersions(maxVersion);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                //创建表
                admin.createTable(hTableDescriptor);
                logger.warn(tableName + "表创建成功");
            } else {
                logger.warn(tableName + "表已存在");
            }
        } finally {
            admin.close();
            connection.close();
        }
    }

    // 删除表
    public static void dropTable(String tableName) throws IOException {
        // 获取hbase table对象
        TableName hTableName = TableName.valueOf(tableName);
        //执行删除
        try {
            if (admin.tableExists(hTableName)) {
                if (admin.isTableEnabled(hTableName)){
                    admin.disableTable(hTableName);
                }
                admin.deleteTable(hTableName);
            } else {
                logger.warn(tableName + "表不存在");
            }
        } finally {
            admin.close();
            connection.close();
        }

    }


}
