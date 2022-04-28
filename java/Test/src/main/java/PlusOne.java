public class PlusOne {
    public static void main(String[] args) {
        int[] nums = new int[]{9,9,9};
        int[] result = plusOne(nums);
        for (int i:result) {
            System.out.println(i);
        }
    }

    public static int[] plusOne(int[] digits){
        for (int i=digits.length-1;i>=0;i--){
            if (digits[i]==9){
                digits[i] = 0;
            }else {
                digits[i] = digits[i]+1;
                break;
            }
        }
        if (digits[0] == 0){
            int[] newNums = new int[digits.length+1];
            newNums[0] = 1;
            return newNums;
        }
        return digits;
    }
}
