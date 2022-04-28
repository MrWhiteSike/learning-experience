import java.util.Arrays;
import java.util.Collections;

public class Test2 {

    public static void main(String[] args) {
//        int[] nums = new int[]{-2,1,-3,4,-1,2,1,-5,4};
        Integer[] nums = new Integer[]{-2,1,-3,4,-1,2,1,-5,4};
        int max = maxSubArray(nums);
        System.out.println(max);
    }

    /**
     * 贪心算法实现：数组中子串的最大和，可以包含单个元素
     * 思想：若当前指针所指元素之前的和小于0，则丢弃当前元素之前的数列
     * @param nums
     * @return
     */
    public static int maxSubArray(int[] nums){
        int sum = nums[0];
        int max = nums[0];
        for(int i=1;i<nums.length;i++){
            sum = Math.max(sum+nums[i], nums[i]);
            max = Math.max(sum, max);
        }
        return max;
    }

    /**
     * 动态规划实现：数组中子串的最大和，可以包含单个元素
     * 思想：若前一个元素大于0，则将其加到当前元素上
     * @param nums
     * @return
     */
    public static int maxSubArray(Integer[] nums){
        for(int i=1;i<nums.length;i++){
            if (nums[i-1] > 0){
                System.out.println("pre = "+nums[i-1]);
                nums[i]+=nums[i-1];
            }
        }
        return Collections.max(Arrays.asList(nums));
    }
}
