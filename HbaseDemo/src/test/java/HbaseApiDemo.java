import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 19856
 * @since 2023/4/11-14:26
 */

public class HbaseApiDemo {
    //初始化Configuration对象
    private Configuration conf=null;
    //初始化连接
    private Connection conn = null;

    @Before
    public void init() throws IOException {
    //获取对象
        conf = HBaseConfiguration.create();
        //设置zookeeper集群地址
        conf.set("hbase.zookeeper.quorum","hadoop100:2181,hadoop101:2181,hadoop102:2181");
        //获取连接
        conn= ConnectionFactory.createConnection(conf);
    }

    //创建表
    @Test
    public void CreateTable() throws Exception{
        try {
            //获取操作对象
            Admin admin = conn.getAdmin();
            //构建一个t_user表
            TableDescriptorBuilder t_user = TableDescriptorBuilder.newBuilder(TableName.valueOf("t_user"));
            //创建列族info
            ColumnFamilyDescriptor of = ColumnFamilyDescriptorBuilder.of("info");
            t_user.setColumnFamily(of);
            //创建列祖data
            ColumnFamilyDescriptor of1 = ColumnFamilyDescriptorBuilder.of("data");
            t_user.setColumnFamily(of1);
            //构建
            TableDescriptor build = t_user.build();
            //创建表
            admin.createTable(build);
            //关闭连接
            admin.close();
            conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //插入数据
    @Test
    public void testPut() throws Exception{
        //1. 定义表的名称
        TableName tableName = TableName.valueOf("t_user");

        //2. 获取表对象
        Table table = conn.getTable(tableName);

        //3. 准备数据
        ArrayList<Put> puts = new ArrayList<>();
        //构建put对象
        Put put01 = new Put(Bytes.toBytes("rk002"));
        put01.addColumn(Bytes.toBytes("info"),Bytes.toBytes("username"),Bytes.toBytes("zhangsan"));
        put01.addColumn(Bytes.toBytes("info"),Bytes.toBytes("password"),Bytes.toBytes("123456"));
        Put put02 = new Put("rk003".getBytes());
        put02.addColumn(Bytes.toBytes("info"),Bytes.toBytes("username"),Bytes.toBytes("list"));
        puts.add(put01);
        puts.add(put02);
        // 4. 添加数据
        table.put(puts);
        table.close();
        conn.close();
    }

//查询表
@Test
public void get() throws IOException {
    // 1.定义表的名称
    TableName tableName = TableName.valueOf("t_user");

    // 2.获取表
    Table table = conn.getTable(tableName);

    // 3.准备数据
    String rowKey = "rk002";

    // 4.拼装查询条件
    Get get = new Get(Bytes.toBytes(rowKey));

    // 5.查询数据
    Result result = table.get(get);

    // 6.打印数据 获取所有的单元格
    List<Cell> cells = result.listCells();
    for (Cell cell : cells) {
        // 打印rowkey,family,qualifier,value
        System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
    }
}
    @Test
    public void scan() throws IOException {
        // 1.定义表的名称
        TableName tableName = TableName.valueOf("t_user");

        // 2.获取表
        Table table = conn.getTable(tableName);

        // 3.全表扫描
        Scan scan = new Scan();

        // 4.获取扫描结果
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;

        // 5. 迭代数据
        while ((result = scanner.next()) != null) {
            // 6.打印数据 获取所有的单元格
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                // 打印rowkey,family,qualifier,value
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
            }
        }
    }

    @Test
    public void testDel() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_user"));
        //获取delete对象,需要一个rowkey
        Delete delete = new Delete("rk002".getBytes());
        //在delete对象中指定要删除的列族-列名称
        delete.addColumn("info".getBytes(), "password".getBytes());
        //执行删除操作
        table.delete(delete);
        System.out.println("删除列成功");
        //关闭
        table.close();
    }



    @Test
    public void testDrop() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //删除表时先需要disable，将表置为不可用，然后在delete
        admin.disableTable(TableName.valueOf("t_user"));
        admin.deleteTable(TableName.valueOf("t_user"));
        System.out.println("删除表成功");
        //关闭
        admin.close();
        conn.close();
    }









}
