package com.gsafety.lifeline.bigdata.znodeWatch;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by hadoop on 2017/9/5.
 */
public class DataListener implements IZkDataListener {
    private static final Logger LOG = LoggerFactory.getLogger(DataListener.class);
    private Map<String, String> regulations;
    private Map<String, IZkDataListener> listeners;
    private ZkClient client;

    public DataListener(ZkClient client, Map<String, String> regulations, Map<String, IZkDataListener> listeners){
        this.regulations = regulations;
        this.listeners = listeners;
        this.client = client;
    }

    public DataListener() {

    }

    public void handleDataChange(String dataPath, Object data) throws Exception {
        String key = dataPath;
        this.regulations.put(key, data.toString());
        LOG.info("child {} change data:{}",key,data);
    }

    public void handleDataDeleted(String s) throws Exception {

    }
}
