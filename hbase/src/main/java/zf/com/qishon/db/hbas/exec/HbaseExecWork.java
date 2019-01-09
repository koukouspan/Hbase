package zf.com.qishon.db.hbas.exec;

import org.apache.hadoop.hbase.client.Result;
import static zf.com.qishon.db.hbas.util.FormatResult.formatRowCell;
import zf.com.qishon.db.hbas.util.HbaseAdOperate;
import zf.com.qishon.db.hbas.util.HbaseDateOperate;


import java.io.IOException;

public class HbaseExecWork {

    public static void main(String[] args) {
        HbaseAdOperate adOperate = new HbaseAdOperate();
        //建表
        String[] familyCl = {"f1", "f2", "f3"};
        try {
            adOperate.createTable("qstest:zf_test", familyCl, 3);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //写入数据
        HbaseDateOperate dateOperate = new HbaseDateOperate();
        try {
            for (int i = 1; i <= 10; i++) {
                dateOperate.putOneValue("qstest:zf_test", "A" + String.valueOf(i), "f1", "name", "name" + String.valueOf(i));
                dateOperate.putOneValue("qstest:zf_test", "A" + String.valueOf(i), "f2", "addr", "addr" + String.valueOf(i));
                dateOperate.putOneValue("qstest:zf_test", "A" + String.valueOf(i), "f3", "phone", "phone" + String.valueOf(i));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        //读取数据
        try {
            Result result= dateOperate.hbaseGetRow("qstest:zf_test","A1");
            formatRowCell(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //读取数据
        try {
            Result result= dateOperate.hbaseGetCell("qstest:zf_test","A1","f1","addr");
            formatRowCell(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dateOperate.admin.close();
            dateOperate.connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
