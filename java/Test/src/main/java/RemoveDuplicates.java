import java.util.ArrayList;
import java.util.TreeSet;

public class RemoveDuplicates {
    public static void main(String[] args) {
        int[] nums = new int[]{0,0,1,1,1,2,2,3,3,4};
        int result = removeDuplicates3(nums);
        System.out.println(result);
    }

    /**
     * 数组重复数据去重：TreeSet 实现
     * @param nums
     * @return
     */
    public static int removeDuplicates(int[] nums) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i=0;i<nums.length;i++){
            treeSet.add(nums[i]);
        }
        int res = treeSet.size();
        int index = 0;
        while (!treeSet.isEmpty()){
            nums[index++] = treeSet.pollFirst();
        }
        return res;
    }

    /**
     * 数组重复数据去重：ArrayList 实现
     * @param nums
     * @return
     */
    public static int removeDuplicates2(int[] nums) {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(nums[0]);
        for (int i=1;i<nums.length;i++){
            if (nums[i-1] != nums[i]){
                list.add(nums[i]);
                nums[list.size()-1] = nums[i];
            }
        }
        return list.size();
    }

    /**
     * 数组重复数据去重：变量标记法
     * @param nums
     * @return
     */
    public static int removeDuplicates3(int[] nums) {
        if (nums.length<=1){
            return nums.length;
        }
        int flag = 1;
        for (int i=1;i<nums.length;i++){
            if (nums[i-1] != nums[i]){
                nums[flag++] = nums[i];
            }
        }
        return flag;
    }
}
