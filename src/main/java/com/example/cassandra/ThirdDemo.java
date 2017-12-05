package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.*;

import java.util.List;

public class ThirdDemo {
    public static void main(String[] args) {

        //定义Cluster
        Cluster cluster = null;
        Session session = null;
        try {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

            //需要获取Session对象
            session = cluster.connect();

            //插入数据
            Insert insert = QueryBuilder.insertInto("testkeyspace1", "student").value("name", "wang").value("age", 30);
            session.execute(insert);

            Select.Where select = QueryBuilder.select().all().from("testkeyspace1", "student").where(QueryBuilder.eq("name", "wang"));
            ResultSet resultSet = session.execute(select);
            List<Row> all = resultSet.all();
            for (Row row : all) {
                System.out.println(">=name: " + row.getString("name"));
                System.out.println(">=age: " + row.getInt("age"));
            }

            //修改
            Update.Where where = QueryBuilder.update("testkeyspace1", "student").with(QueryBuilder.set("age", 45)).where(QueryBuilder.eq("name", "wang"));

            session.execute(where);
            ResultSet resultSet1 = session.execute(select);
            List<Row> all1 = resultSet1.all();
            for (Row row : all1) {
                System.out.println(">=name: " + row.getString("name"));
                System.out.println(">=age: " + row.getInt("age"));
            }

            Delete.Where where1 = QueryBuilder.delete().from("testkeyspace1", "student").where(QueryBuilder.eq("name", "wang"));
            session.execute(where1);

            ResultSet resultSet2 = session.execute(select);
            List<Row> all2 = resultSet2.all();
            for (Row row : all2) {
                System.out.println(">=name: " + row.getString("name"));
                System.out.println(">=age: " + row.getInt("age"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
            cluster.close();
        }

    }
}
