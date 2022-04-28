import java.util.*;

public class Test1 {
    public static void main(String[] args) {
//        HashMap<String, String> map = new HashMap<String, String>();
//        map.put("key1","value1");
//        map.put("key2","value2");
//        HashMap<String, String> map1 = new HashMap<String, String>();
//        map1.put("keyA","valueA");
//        map1.put("d",new int[]{3,4});
//        map.put("b",map1);
//        Map<String, Object> map2 = transformMap(map);
//        System.out.println(map2.get("b.c"));
//        HashMap[] arr = new HashMap[]{map,map1};
//        String text = store(arr);
//
//        HashMap[] arr2 = load(text);
//        for(int i=0;i<arr2.length;i++){
//            System.out.println(arr2[i].entrySet());
//        }
        String str1 = "Welcomn";
        String str2 = "lo";
        System.out.println(minStr2(str1, str2));
    }

    /**
     * 多层嵌套Map展开成一层Map，通过递归调用
     * @param map
     * @return
     */
    public static Map<String, Object> transformMap(Object map){
        Map<String,Object> map1 = (Map<String, Object>) map;
        Map<String, Object> result = new HashMap<String, Object>();
        for(String key:map1.keySet()){
            if(map1.get(key) instanceof Map){
                Map<String, Object> nextResult = transformMap(map1.get(key));
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

    /**
     * HashMap数组存储为字符串，每个Map存为一行（\n分隔），Map的中键值对之间以；隔开，键和值以=分隔
     * @param arr
     * @return
     */
    public static String store(HashMap<String,String>[] arr){
        StringBuilder builder = new StringBuilder();
        for(int i=0;i<arr.length;i++){
            HashMap<String,String> map = arr[i];
            for (String key:map.keySet()) {
                builder.append(key+"="+map.get(key)+";");
            }
            if(builder.lastIndexOf(";") == builder.length()-1){
                builder.replace(builder.length()-1,builder.length(),"");
            }
            builder.append("\n");
        }
        if(builder.lastIndexOf("\n") == builder.length()-1){
            builder.replace(builder.length()-1,builder.length(),"");
        }

        return builder.toString();
    }

    /**
     * 加载数据：输入text字符串，转换为HashMap数组形式
     * @param text
     * @return
     */
    public static HashMap<String,String>[] load(String text){
        List<HashMap<String,String>> list = new ArrayList<>();
        String[] mapString = text.split("\n");
        for (String map:mapString) {
            String[] kvs = map.split(";");
            HashMap<String, String> hashMap = new HashMap<>();
            for (String kv:kvs) {
                String[] entry = kv.split("=");
                hashMap.put(entry[0],entry[1]);
            }
            list.add(hashMap);
        }
        HashMap<String, String>[] arr = new HashMap[list.size()];
        for (int i=0;i<list.size();i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }


    /**
     * 找出字符串中，包含n个字符的最小子串
     * 暴力解法：先找出符合的子串，然后进行长度排序，获取最短子串
     * @param str1
     * @param str2
     * @return
     */
    public static String minStr(String str1, String str2){
        char[] chars1 = str1.toCharArray();
        char[] chars2 = str2.toCharArray();
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();
        for(int i=0;i<chars1.length;i++){
            for(int j=i+chars2.length;j<chars1.length;j++){
                int flag = 0;
                String temp = str1.substring(i,j);
                for(int z=0;z<chars2.length;z++){
                    if(!str1.substring(i,j).contains(String.valueOf(chars2[z]))){
                        break;
                    }
                    flag++;
                }
                if(flag == chars2.length){
                    map.put(temp.length(), temp);
                }
            }
        }
        return map.size() == 0?"":map.get(map.firstKey());
    }

    /**
     * 简单解法：从最小的子串开始遍历，找到就返回
     * @param str1
     * @param str2
     * @return
     */
    public static String minStr2(String str1, String str2){
        char[] chars1 = str1.toCharArray();
        char[] chars2 = str2.toCharArray();
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();
        int len = chars2.length;
        while (len<=chars1.length) {
            for(int i=0;i <= chars1.length-len;i++){
                int flag = 0;
                String temp = str1.substring(i,i+len);
                for(int z=0;z<chars2.length;z++){
                    if(!temp.contains(String.valueOf(chars2[z]))){
                        break;
                    }
                    flag++;
                }
                if(flag == chars2.length){
                    return temp;
                }
            }
            len++;
        }
        return "";
    }
}
