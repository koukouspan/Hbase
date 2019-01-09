package zf.com.qishon.db.hbas.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zf on 2019/1/9.
 */
public class FormatResult {
    public static void formatRowCell(Result result){
        Cell[] cells =result.rawCells();
        for (Cell cell:cells) {
            System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp: " + new String(cell.getTimestamp() + " "));
            System.out.println("columnFamily: " + new String(Bytes.toString(CellUtil.cloneFamily(cell)) + " "));
            System.out.println("columnName: " + new String(Bytes.toString(CellUtil.cloneQualifier(cell)) + " "));
            System.out.println("value: " + new String(Bytes.toString(CellUtil.cloneValue(cell)) + " "));
            System.out.println("--------------------------------------------");
        }

    }
}
