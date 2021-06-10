import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/10 下午6:31
 * @email jsjxzw@163.com
 */
public class HBaseDemo {

    Configuration conf = null;
    Connection conn = null;
    // 表的管理对象
    Admin admin = null;
    // 创建表的独享
    TableName tableName = TableName.valueOf("phone");

    @Before
    public void init() throws IOException {
        // 创建配置文件对象
        conf = HBaseConfiguration.create();
        // 加载zookeeper的配置
        conf.set("hbase.zookeeper.quorum","node01,node02,node03");
        // 获取连接
        conn = ConnectionFactory.createConnection(conf);
        // 获取对象
        admin = conn.getAdmin();
    }

    @Test
    public void createTable() throws IOException {
        // 定义表描述对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        // 定义列族描述对象
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
        // 添加列族信息给表
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        // 创建表
        admin.createTable(tableDescriptorBuilder.build());

    }

    @After
    public void destory() throws IOException {
        admin.close();
        conn.close();
    }

}
