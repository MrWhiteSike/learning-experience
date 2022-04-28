public class StrStr {
    public static void main(String[] args) {
        String haystack = "a";
        String needle = "a";

        System.out.println(strStr(haystack,needle));

    }
    public static int strStr(String haystack, String needle) {
        if (haystack == null || haystack.equals("") || needle == null || needle.equals("")){
            return 0;
        }
        int len = needle.length();
        for (int i=0;i<=haystack.length()-len;i++){
            if (haystack.substring(i,i+len).equals(needle)){
                return i;
            }
        }
        return -1;
    }
}
