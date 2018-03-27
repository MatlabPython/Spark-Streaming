package com.gsafety.lifeline.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

/**
 * Created by chiyong on 2017/5/13.
 */
public class HBaseConnection {

    private static Logger logger= Logger.getLogger(HBaseConnection.class);
    private static Connection connection=null;




    public static  Connection  getConnection(){
        logger.info(" getConnection : connection:"+connection);
        return connection;

    }


    public static Connection createConnecion(String hbaseZookeeperPort,String zookeeperquorum ){
        try {
            Configuration conf = HBaseConfiguration.create();

            conf.set("hbase.zookeeper.property.clientPort", hbaseZookeeperPort);
            conf.set("hbase.zookeeper.quorum",
                    zookeeperquorum);
           /* String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            System.out.println("---++"+userdir);
            conf.addResource(new Path(userdir + "core-site.xml"));
            conf.addResource(new Path(userdir + "hdfs-site.xml"));
            conf.addResource(new Path(userdir + "hbase-site.xml"));*/
            connection = ConnectionFactory.createConnection(conf);

        }catch (Exception e){
            logger.error("init HBase Comnnection error",e);
        }

        return connection;
    }

}
