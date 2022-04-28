public class ClimbStairs {
    public static void main(String[] args) {
        System.out.println(climbStairs(3));
    }

    /**
     * 爬楼：动态规划
     * fn = fn-1+fn-2
     * 边界条件：从0级开始爬的，所以第0级爬到第0级只有一种方案 f（0）=1
     * 从 0级到第1级也只有一种方案，f(1)=1
     * 根据转移方程得到 f(2) = 2,f(3) = 3, f(4) = 5, f(5)=8
     * @param n
     * @return
     */
    public static int climbStairs(int n){
        int p =0,q =0, r =1;
        for (int i=1;i<=n;i++){
            p = q;
            q = r;
            r = p+q;
        }
        return r;
    }

}
