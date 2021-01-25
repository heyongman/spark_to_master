package com.he.ser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class KryoTest {
    public static void main(String[] args) {
        final Kryo kryo = new Kryo();
        kryo.register(XModel.class);
        kryo.register(Model.class);
        kryo.register(byte[].class);
        kryo.register(HashMap.class);

        final long s = System.nanoTime();
        for (int i = 0; i < 1000000; i++) {
            final ByteBufferOutput output = new ByteBufferOutput(64,-1);
            final HashMap<String, String> map = new HashMap<>();
            map.put("aa","fafa房贷首付");
            final Model model = new XModel(map,"adfaafs士大夫"+i, 11+i, "rfsef十分舒服的第三方".getBytes(StandardCharsets.UTF_8));
            kryo.writeClassAndObject(output,model);
            final Object object = kryo.readClassAndObject(new ByteBufferInput(output.getByteBuffer()));
//            System.out.println(object instanceof XModel);
//            System.out.println(object);
        }
        final long e = System.nanoTime();
        System.out.println((e-s)/1000000.0+" ms");
    }
}

class Model{
    private Map<String,String> header;
    private String name;
    private Integer status;
    private byte[] data;

    public Model() {
    }

    public Model(Map<String,String> header,String name, Integer status, byte[] data) {
        this.header = header;
        this.name = name;
        this.status = status;
        this.data = data;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Model{" +
                "name='" + name + '\'' +
                ", status=" + status +
                ", data=" + new String(data,StandardCharsets.UTF_8) +
                '}';
    }
}

class XModel extends Model{
    public XModel() {
    }

    public XModel(Map<String,String> header,String name, Integer status, byte[] data) {
        super(header, name, status, data);
    }
}