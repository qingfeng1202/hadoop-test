package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {

    // 表的管理类
    Admin admin = null;
    // 数据的管理类
    HTable table = null;
    // 表名
    String tm = "phone";

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
     * 新增数据
     */
    @Test
    public void insert() throws Exception {
        // rowkey
        Put put = new Put("222".getBytes());

        put.addColumn("cf".getBytes(), "name".getBytes(), "lisi".getBytes());
        put.addColumn("cf".getBytes(), "age".getBytes(), "15".getBytes());
        put.addColumn("cf".getBytes(), "sex".getBytes(), "man".getBytes());

        table.put(put);
        table.flushCommits();
    }

    /**
     * 获取数据
     * @throws Exception
     */
    @Test
    public void get() throws Exception {
        Get get = new Get("222".getBytes());
        // 添加要获取的列和列族，减少网络的io, 相当于在服务器端做了过滤
        get.addColumn("cf".getBytes(), "name".getBytes());
        get.addColumn("cf".getBytes(), "age".getBytes());
        get.addColumn("cf".getBytes(), "sex".getBytes());

        Result result = table.get(get);

        Cell cell = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
        Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
        System.out.println( Bytes.toString( CellUtil.cloneValue(cell)));
        System.out.println( Bytes.toString( CellUtil.cloneValue(cell1)));
        System.out.println( Bytes.toString( CellUtil.cloneValue(cell2)));

    }


    @Test
    public void scan() throws Exception{
        Scan scan = new Scan();

        scan.setMaxVersions(3);//设置读取的最大的版本数
//        scan.setStartRow();
//        scan.setStopRow();

        ResultScanner rss = table.getScanner(scan);
        for (Result result : rss){
            Cell cell = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
            Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
            Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
            System.out.println( Bytes.toString( CellUtil.cloneValue(cell)));
            System.out.println( Bytes.toString( CellUtil.cloneValue(cell1)));
            System.out.println( Bytes.toString( CellUtil.cloneValue(cell2)));
        }
    }


}
