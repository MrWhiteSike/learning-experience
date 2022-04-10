package com.bsk.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * 输入数据：hello，hive
 * 输出；
 *      hello
 *      hive
 */

public class MyUDTF extends GenericUDTF {

    // 输出数据的集合
    private ArrayList<String> outPutList = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 输出数据的默认列名，可以被别名覆盖
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");
        // 输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        // 最终返回值
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    // 处理输入数据
    @Override
    public void process(Object[] objects) throws HiveException {
        // 1.取出输入数据
        String input = objects[0].toString();
        String split = objects[1].toString();

        // 2.按照传入的分隔符进行分隔，这样更加灵活
        String[] words = input.split(split);

        // 3.遍历数据写出: 每一个数据都被包装成List集合输出
        for (String word : words) {
            // 清空集合
            outPutList.clear();
            // 将数据放入集合
            outPutList.add(word);
            // 输出数据
            forward(outPutList);
        }

    }

    // 收尾方法
    @Override
    public void close() throws HiveException {

    }
}