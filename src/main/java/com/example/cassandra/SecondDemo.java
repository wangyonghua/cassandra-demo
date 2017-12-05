package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

public class SecondDemo {
    public static void main(String[] args) {

        //定义Cluster
        Cluster cluster = null;
        Session session = null;
        try {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

            //需要获取Session对象
            session = cluster.connect();

            //创建键空间
            String createKeySpaceCQL = "create keyspace if not exists testkeyspace1 with replication={'class':'SimpleStrategy','replication_factor':1}";
            session.execute(createKeySpaceCQL);

            //创建列簇
            String createTableCQL = "create table if not exists testkeyspace1.student(name varchar primary key,age int)";

            session.execute(createTableCQL);

            //插入数据
            String insertCQL = "insert into testkeyspace1.student(name,age) values('zhangsha',20)";
            session.execute(insertCQL);

            String selectCQL = "select * from testkeyspace1.student";
            ResultSet resultSet = session.execute(selectCQL);
            List<Row> all = resultSet.all();
            for (Row row : all) {
                System.out.println(">=name: " + row.getString("name"));
                System.out.println(">=age: " + row.getInt("age"));
            }

            //修改
            String updateCQL = "update testkeyspace1.student set age=21 where name='zhangsha'";
            session.execute(updateCQL);

//            String deleteCQL = "delete from testkeyspace1.student where name='zhangsha'";
//            session.execute(deleteCQL);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
            cluster.close();
        }

    }
}
