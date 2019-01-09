package zf.com.qishon.db.hbas.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;


public class HbaseConnection {
    //日志类
    private  static  final Logger logger =LoggerFactory.getLogger(HbaseConnection.class);
    //hadoop 设置
    private static Configuration configuration;
    //hbase 连接
    private static Connection connection;

    //获取hadoop 配置
    public static Configuration getConfiguration(){
        if(configuration == null){
            configuration = HBaseConfiguration.create();
        }
        return  configuration;
    }
    // 连接hbase
    public static synchronized Connection getConnection(){
        //加载配置文件
        getConfiguration();
        //获取连接
        if(connection==null){
            try {
                connection =ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

}
