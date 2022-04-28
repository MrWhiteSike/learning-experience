public class AddBinary {
    public static void main(String[] args) {
        String a = "1010",b = "1011";
        System.out.println(addBinary(a,b));
    }
    public static String addBinary(String a, String b) {
        // 思路1：

        // 1.将二进制转换为10进制
        // 2.两个十进制加和
        // 3.将加和的十进制转换为二进制

        // 思路2：
        // 逐位相加
        int length = Math.max(a.length(),b.length());
        StringBuilder maxStr = new StringBuilder();
        StringBuilder minStr = new StringBuilder();
        if (a.length() == length){
            for (int i=0;i<length-b.length();i++){
                minStr.append("0");
            }
            minStr.append(b);
            maxStr.append(a);
        }else {
            for (int i=0;i<length-a.length();i++){
                minStr.append("0");
            }
            minStr.append(a);
            maxStr.append(b);
        }
        String str1 = maxStr.toString();
        String str2 = minStr.toString();
        StringBuilder result = new StringBuilder();
        int flag = 0;
        for (int i=length-1;i>=0;i--){
            int n1 = Integer.parseInt(str1.substring(i,i+1));
            int n2 = Integer.parseInt(str2.substring(i,i+1));
            int temp = n1+n2+flag;
            result.append(temp % 2);
            if (temp >= 2){
                flag = 1;
                if (i==0){
                    result.append("1");
                }
            }else {
                flag = 0;
            }
        }
        return result.reverse().toString();
    }

}
