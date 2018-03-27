import com.gsafety.lifeline.bigdata.znodeWatch.DataListener;
import com.gsafety.lifeline.bigdata.znodeWatch.SystemConfig;

import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hadoop on 2017/9/4.
 */
public class TestZnode {
    public static void main(String[] args) throws InterruptedException {
        Map<String, String> regulations = new ConcurrentHashMap<String, String>();
        final String[] aaa = {""};
        ZkClient zkClient = new ZkClient(SystemConfig.get("ZK_LIST"),10000,10000,ZKStringSerializer$.MODULE$);
        zkClient.subscribeDataChanges(SystemConfig.get("ZK_DYNAMIC_CONFIG"), new DataListener() {
            public void handleDataChange(String s, Object o) throws Exception {
                aaa[0] = o+"";
                System.out.println("节点变更为："+s+",数据变更为："+o);
            }

            public void handleDataDeleted(String s) throws Exception {
                System.out.println("删除节点为："+s);
            }
        });
        System.out.println("-=-"+aaa[0]);
        System.out.println(zkClient.readData(SystemConfig.get("ZK_DYNAMIC_CONFIG")));
        Thread.sleep(1000000);
        System.out.println(zkClient.readData(SystemConfig.get("ZK_DYNAMIC_CONFIG")));



    }
}
