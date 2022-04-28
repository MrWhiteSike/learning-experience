import java.util.Stack;

public class YouXiaoKuoHao {
    public static void main(String[] args) {
        String s = "([)]";
        System.out.println(isValid(s));
    }

    /**
     * 有效的括号问题：使用栈的数据结构来解决，栈的特点是先进后出；队列的特点是先进先出
     * @param s
     * @return
     */
    public static boolean isValid(String s){
        Stack<Character> stack = new Stack<>();
        for (int i=0;i<s.length();i++){
            char c = s.charAt(i);
            if (c == '(' || c=='[' || c=='{')
                stack.push(c);
            else if (stack.isEmpty() || c==')' && stack.pop() != '(' ||c==']'&&stack.pop()!='['||c=='}'&&stack.pop()!='{')
                return false;
        }
        return stack.isEmpty();
    }
}
