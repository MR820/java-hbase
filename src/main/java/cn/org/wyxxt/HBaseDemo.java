package cn.org.wyxxt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** 如果遇到特别慢的情况，设置DNS服务器地址 223.5.5.5 223.6.6.6 */

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
  Table table = null;
  // 创建表的独享
  TableName tableName = TableName.valueOf("phone");

  @Before
  public void init() throws IOException {
    // 创建配置文件对象
    conf = HBaseConfiguration.create();
    // 加载zookeeper的配置
    conf.set("hbase.zookeeper.quorum", "node01,node02,node03");
    // 获取连接
    conn = ConnectionFactory.createConnection(conf);
    // 获取对象
    admin = conn.getAdmin();
    // 获取数据操作对象
    table = conn.getTable(tableName);
  }

  @Test
  public void createTable() throws IOException {
    // 定义表描述对象
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    // 定义列族描述对象
    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
    // 添加列族信息给表
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    // 创建表
    admin.createTable(tableDescriptorBuilder.build());
  }

  @Test
  public void insert() throws IOException {
    Put put = new Put(Bytes.toBytes("22222"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("14"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes("women"));
    table.put(put);
  }

  @Test
  public void get() throws IOException {
    Get get = new Get(Bytes.toBytes("1111"));
    // 在服务端做数据过滤，挑选出符合需求的列
    //    get.addFamily(Bytes.toBytes("cf"));
    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"));
    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
    Result result = table.get(get);
    Cell cell1 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
    Cell cell2 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
    Cell cell3 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
    String name = Bytes.toString(CellUtil.cloneValue(cell1));
    String age = Bytes.toString(CellUtil.cloneValue(cell2));
    String sex = Bytes.toString(CellUtil.cloneValue(cell3));
    System.out.println(name);
    System.out.println(age);
    System.out.println(sex);
  }

  /** 获取表中所有的记录 */
  @Test
  public void scan() throws IOException {
    Scan scan = new Scan();
    ResultScanner rss = table.getScanner(scan);
    for (Result rs : rss) {
      Cell cell1 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
      Cell cell2 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
      Cell cell3 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
      String name = Bytes.toString(CellUtil.cloneValue(cell1));
      String age = Bytes.toString(CellUtil.cloneValue(cell2));
      String sex = Bytes.toString(CellUtil.cloneValue(cell3));
      System.out.println(name);
      System.out.println(age);
      System.out.println(sex);
    }
  }

  /**
   * 通过记录：
   *
   * <p>字段 手机号 时间 时长 对方手机号 类型（主叫/被叫）
   *
   * <p>RowKey 手机号（倒叙）+ 时间戳（Long.Max-）
   */
  /** 假设有10个用户，每个用户一年产生10000条记录 */
  @Test
  public void insertManyData() throws ParseException, IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String phoneNumber = getNumber("158");
      for (int j = 0; j < 10000; j++) {
        String dnum = getNumber("177");
        String length = String.valueOf(random.nextInt(100));
        String date = getDate("2019");
        String type = String.valueOf(random.nextInt(2));
        // rowKey
        String rowKey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("dnum"), Bytes.toBytes(dnum));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("length"), Bytes.toBytes(length));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(date));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes(type));
        puts.add(put);
      }
    }
    table.put(puts);
  }

  @Test
  public void insertByProtoBuf() throws ParseException, IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String phoneNumber = getNumber("158");
      for (int j = 0; j < 10000; j++) {
        String dnum = getNumber("177");
        String length = String.valueOf(random.nextInt(100));
        String date = getDate("2019");
        String type = String.valueOf(random.nextInt(2));
        // rowKey
        String rowKey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());

        Phone.PhoneDetail.Builder builder = Phone.PhoneDetail.newBuilder();
        builder.setDate(date);
        builder.setDnum(dnum);
        builder.setLength(length);
        builder.setType(type);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("phone"), builder.build().toByteArray());
        puts.add(put);
      }
    }
    table.put(puts);
  }

  @Test
  public void getByProtoBuf() throws IOException {
    Get get = new Get(Bytes.toBytes("15894400392_9223370487438904807"));
    Result rs = table.get(get);
    byte[] b =
        CellUtil.cloneValue(rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("phone")));
    Phone.PhoneDetail phoneDetail = Phone.PhoneDetail.parseFrom(b);
    System.out.println(phoneDetail);
  }

  /**
   * 查询某一个用户3月份的通话记录
   *
   * @throws IOException
   */
  @Test
  public void scanByCondition() throws IOException, ParseException {
    Scan scan = new Scan();
    String startRow =
        "15897566746" + "_" + (Long.MAX_VALUE - sdf.parse("20190331000000").getTime());
    String stopRow = "15897566746" + "_" + (Long.MAX_VALUE - sdf.parse("20190301000000").getTime());
    scan.withStartRow(Bytes.toBytes(startRow));
    scan.withStopRow(Bytes.toBytes(stopRow));
    ResultScanner rss = table.getScanner(scan);
    for (Result rs : rss) {
      Cell cell1 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("dnum"));
      Cell cell2 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("length"));
      Cell cell3 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
      Cell cell4 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
      String dnum = Bytes.toString(CellUtil.cloneValue(cell1));
      String length = Bytes.toString(CellUtil.cloneValue(cell2));
      String date = Bytes.toString(CellUtil.cloneValue(cell3));
      String type = Bytes.toString(CellUtil.cloneValue(cell4));
      System.out.println(dnum + "--" + length + "--" + date + "--" + type);
    }
  }

  /** 查询某个用户所有的主叫电话(type=1) */
  @Test
  public void getType() throws IOException {
    Scan scan = new Scan();
    // 创建过滤器集合
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    // 创建过滤器
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter(
            Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareOperator.EQUAL, Bytes.toBytes("1"));
    filters.addFilter(filter1);
    // 前缀过滤器
    PrefixFilter filter2 = new PrefixFilter(Bytes.toBytes("15897566746"));
    filters.addFilter(filter2);
    scan.setFilter(filters);
    ResultScanner rss = table.getScanner(scan);
    for (Result rs : rss) {
      Cell cell1 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("dnum"));
      Cell cell2 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("length"));
      Cell cell3 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
      Cell cell4 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
      String dnum = Bytes.toString(CellUtil.cloneValue(cell1));
      String length = Bytes.toString(CellUtil.cloneValue(cell2));
      String date = Bytes.toString(CellUtil.cloneValue(cell3));
      String type = Bytes.toString(CellUtil.cloneValue(cell4));
      System.out.println(dnum + "--" + length + "--" + date + "--" + type);
    }
  }

  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  private String getDate(String s) {
    return s
        + String.format(
            "%02d%02d%02d%02d%02d",
            random.nextInt(12) + 1,
            random.nextInt(31),
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60));
  }

  Random random = new Random();

  public String getNumber(String str) {
    return str + String.format("%08d", random.nextInt(99999999));
  }

  @Test
  public void delete() throws IOException {
    Delete delete = new Delete(Bytes.toBytes("111"));
    table.delete(delete);
  }

  @After
  public void destory() throws IOException {
    table.close();
    admin.close();
    conn.close();
  }
}
