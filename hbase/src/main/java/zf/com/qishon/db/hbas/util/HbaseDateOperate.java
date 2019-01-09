package zf.com.qishon.db.hbas.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


//数据操作类
public class HbaseDateOperate {

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
            System.out.println("连接成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putOneValue(String tableName ,String rowKey ,String familyCol ,String column ,String value) throws IOException {

        TableName hTableName =TableName.valueOf(tableName);
        System.out.println(admin.tableExists(TableName.valueOf("zftest")));

        if(admin.tableExists(hTableName)){
            Table hTable = null;
            try {
                hTable =connection.getTable(hTableName);
            }catch (IOException e){
                logger.error("连接失败",e);

            }
            Put put =new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyCol),Bytes.toBytes(column),Bytes.toBytes(value));
            hTable.put(put);
            logger.info("写入完成");
            hTable.close();
        }
        else {
            logger.error(tableName+"表不存在");
        }
    }

    //多条put ? 好像有其他方法 后面找

    // 单条get
    public static Result hbaseGetRow(String tableName ,String rowKey) throws  IOException{
        Result result ;
        Table table =connection.getTable(TableName.valueOf(tableName));
        Get get =new Get(Bytes.toBytes(rowKey));
        result =table.get(get);
        table.close();
        return result;
    }

    //获取指定cell
    public static Result hbaseGetCell(String tableName ,String rowKey ,String familyCol ,String column) throws IOException{
        Result result ;
        Table table =connection.getTable(TableName.valueOf(tableName));
        Get get =new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyCol),Bytes.toBytes(column));
        result =table.get(get);
        table.close();
        return result;
    }

}
