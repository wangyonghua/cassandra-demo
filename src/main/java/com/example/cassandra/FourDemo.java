package com.example.cassandra;

import com.datastax.driver.core.*;

import java.util.List;

public class FourDemo {
    public static void main(String[] args) {

        //定义Cluster
        Cluster cluster = null;
        Session session = null;
        try {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

            //需要获取Session对象
            session = cluster.connect();
            PreparedStatement preparedStatement = session.prepare("insert into testkeyspace1.student(name,age) values (?,?)");
            BoundStatement wangyongguya = preparedStatement.bind("wangyongguya", 20);
            session.execute(wangyongguya);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
            cluster.close();
        }

    }
}
