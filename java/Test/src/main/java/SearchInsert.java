public class SearchInsert {
    public static void main(String[] args) {
        int[] nums = new int[]{1,3,5,6};
        int target = 7;
        System.out.println(searchInsert(nums, target));
    }
    public static int searchInsert(int[] nums, int target) {
        int length = nums.length;
        int half = length/2;
        int index = 0;
        if (nums[half]>target){
            for (int i=half-1;i>=0;i--){
                if (target>nums[i]){
                    index = i+1;
                    break;
                }
            }
        }else {
            for (int i=half;i<length;i++){
                if (target<=nums[i]){
                    index = i;
                    break;
                }
                if (i==length-1){
                    index = length;
                }
            }
        }
        return index;
    }

}
