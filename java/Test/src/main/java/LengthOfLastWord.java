public class LengthOfLastWord {
    public static void main(String[] args) {
        String s = "Hello World";
        System.out.println(lengthOfLastWord(s));
    }

    public static int lengthOfLastWord(String s) {
        String str = s.trim();
        int length  = 0;
        for (int i=str.length()-1;i>=0;i--){
            if(str.charAt(i) == ' '){
                break;
            }
            length++;
        }
        return length;
    }
}
