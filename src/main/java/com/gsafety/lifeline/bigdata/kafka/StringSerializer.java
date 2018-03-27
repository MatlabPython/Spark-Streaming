package com.gsafety.lifeline.bigdata.kafka;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import java.io.UnsupportedEncodingException;
/**
 * Created with Intellij IDEA.
 * User: naichun
 * Date: 2018-01-09
 * Time: 19:38
 */
public class StringSerializer implements ZkSerializer {
    private String charset = "UTF-8";
    public StringSerializer(){}
    public StringSerializer(String charset){
        this.charset = charset;
    }
    public byte[] serialize(Object data) throws ZkMarshallingError {
        try{
            byte[] bytes = String.valueOf(data).getBytes(charset);
            return bytes;
        }catch (UnsupportedEncodingException e){
            throw new ZkMarshallingError("Wrong Charset:" + charset);
        }
    }
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        String result = null;
        try {
            result = new String(bytes,charset);
        } catch (UnsupportedEncodingException e) {
            throw new ZkMarshallingError("Wrong Charset:" + charset);
        }
        return result;
    }
}