package zf.com.qishon.db.hbas.exec;

import zf.com.qishon.db.hbas.util.HbaseAdOperate;
import zf.com.qishon.db.hbas.util.HbaseDateOperate;

import java.io.IOException;

public class HbaseExecWork {


    public static void main(String[] args) {
        HbaseAdOperate adOperate = new HbaseAdOperate();
        //建表
        String[] familyCl ={"f1","f2","f3"};
        try {
            adOperate.createTable("zf_test",familyCl,3);
            adOperate.connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
