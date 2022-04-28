public class LongestCommonPrefix {
    public static void main(String[] args) {
        String[] strs = new String[]{"flower","flow","flight"};
        String str = longestCommonPrefix(strs);
        System.out.println(str);
    }

    /**
     * 最长公共前缀：最短字符串长度，
     * @param strs
     * @return
     */
    public static String longestCommonPrefix(String[] strs) {
        int minLen =strs[0].length();
        // 计算字符串数组中，最短字符串的长度
        for(int i=1;i<strs.length;i++){
            minLen = Math.min(minLen, strs[i].length());
        }
        // 获取公共前缀的下标，然后截取返回
        int index = 0;
        for(int i=0;i<minLen;i++){
            int len = 0;
            for(int j=0;j<strs.length-1;j++){
                if(strs[j].charAt(i) != strs[j+1].charAt(i)){
                    break;
                }
                len++;
            }
            if(len == strs.length-1){
                index++;
            }else {
                break;
            }
        }
        return strs[0].substring(0,index);
    }

    /**
     * 采用纵向扫描实现：
     * 思路：
     * 1、获取第一个字符串的长度，以及字符数组的个数
     * 2、外层循环遍历获取第一个字符串的字符
     * 3、内层循环从数组的第二个字符串开始循环，判断除第一个字符串外其他字符串相对应的字符是否相等
     *  如果相等，继续循环；如果其他字符串的个数少于第一个字符串的长度 或者 对应的字符不相等 就直接返回最长公共前缀；
     *
     * @param strs
     * @return
     */
    public static String longestCommonPrefix2(String[] strs) {
        if (strs == null || strs.length == 0){
            return "";
        }
        int length = strs[0].length();
        int count = strs.length;
        for (int i=0;i<length;i++){
            char c = strs[0].charAt(i);
            for (int j=1;j<count;j++){
                if (i==strs[j].length() || strs[j].charAt(i) != c){
                    return strs[0].substring(0,i);
                }
            }
        }
        return strs[0];
    }



}
