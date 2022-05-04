package com.bsk.hive;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("a",1);
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("c",2);
        map1.put("d",new int[]{3,4});
        map.put("b",map1);
        System.out.println(map.get("b.c"));
    }

    public static Map<String, Object> transformMap(Object map){
        Map<String,Object> map1 = (Map<String, Object>) map;
        Map<String, Object> result = new HashMap<String, Object>();
        for(String key:map1.keySet()){
            if(map1.get(key) instanceof Map){
                Map<String, Object> nextResult = transformMap((Map<String, Object>) map1.get(key));
                for(String nkey:nextResult.keySet()){
                    result.put(key+"."+nkey,nextResult.get(nkey));
                }
            }
            else{
                result.put(key,map1.get(key));
            }
        }
        return result;
    }

}
