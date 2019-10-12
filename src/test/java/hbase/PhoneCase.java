package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PhoneCase {

    // 表的管理类
    Admin admin = null;
    // 数据的管理类
    HTable table = null;
    // 表名
    String tm = "wc";

    /**
     * 初始化功能
     * @throws Exception
     */
    @Before
    public void init() throws Exception {
        Configuration conf = HBaseConfiguration.create();// 配置
        conf.set("hbase.zookeeper.quorum", "node10");// zookeeper地址
//        conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口

        Connection connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        table = new HTable(TableName.valueOf(tm), connection);
    }

    @After
    public void destory() throws Exception {
        if(admin != null){
            admin.close();
        }
    }

    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        // 表的描述类
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tm));
        // 列族的描述类
        HColumnDescriptor family = new HColumnDescriptor("cf");
        desc.addFamily(family);
        // 判断是否存在该表
        if(admin.tableExists(TableName.valueOf(tm))){
            admin.disableTable(TableName.valueOf(tm));
            // 删除表
            admin.deleteTable(TableName.valueOf(tm));
        }
        // 创建表
        admin.createTable(desc);
    }

    /**
     * 10个用户，每个用户每年产生1000条通话记录
     *
     * dnum:对方手机号 type:类型：0主叫，1被叫 length：长度 date:时间
     *
     * @throws Exception
     *
     */
    @Test
    public void insert() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for(int i=0; i<10; i++){
            String phoneNumber = getPhone("158");
            for(int j=0; j<1000; j++){
                // 属性
                String dnum = getPhone("177");
                String length = String.valueOf(r.nextInt(99));
                String type = String.valueOf(r.nextInt(2));
                String date = getDate("2018");
                // rowkey设计
                String rowkey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
                Put put = new Put(rowkey.getBytes());
                put.addColumn("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.addColumn("cf".getBytes(), "length".getBytes(), length.getBytes());
                put.addColumn("cf".getBytes(), "type".getBytes(), type.getBytes());
                put.addColumn("cf".getBytes(), "date".getBytes(), date.getBytes());
                puts.add(put);
            }
        }
        table.put(puts);
    }


    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

    private String getDate(String string) {
        return string + String.format("%02d%02d%02d%02d%02d", r.nextInt(12) + 1, r.nextInt(31), r.nextInt(24),
                r.nextInt(60), r.nextInt(60));
    }

    Random r = new Random();
    private String getPhone(String phonePrefix){
        return phonePrefix + String.format("%08d", r.nextInt(99999999));
    }

    /**
     * 查询某一个用户3月份的所有通话记录 条件： 1、某一个用户 2、时间
     *
     * @throws Exception
     */
    @Test
    public void scan() throws Exception {
        String phoneNumber = "15889540013";
        String startRow = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20180401000000").getTime());
        String stopRow = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20180301000000").getTime());

        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.print(Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("--" + Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.print("--" + Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
            System.out.println("--" + Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
        }
    }

    /**
     * 查询某一个用户。所有的主叫电话 条件： 1、电话号码 2、type=0
     *
     * @throws Exception
     */
    @Test
    public void scan2() throws Exception {
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(),
                CompareFilter.CompareOp.EQUAL, "0".getBytes());
        PrefixFilter filter2 = new PrefixFilter("15889540013".getBytes());
        filters.addFilter(filter1);
        filters.addFilter(filter2);

        Scan scan = new Scan();
        scan.setFilter(filters);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.print(Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("--" + Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.print("--" + Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
            System.out.println("--" + Bytes
                    .toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
        }
        scanner.close();
    }

}
