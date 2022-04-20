import java.util.ArrayList;
import java.util.List;

public class ZhongXuBianLi {
    public static void main(String[] args) {

    }

    /**
     * 二叉树中序遍历；递归调用实现；官方实现
     * @param root
     * @return
     */
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<>();
        incoder(root,res);
        return res;
    }

    public void incoder(TreeNode root, List<Integer> res){
        // 递归终止条件：碰到空节点
        if (root == null){
            return;
        }
        incoder(root.left, res);
        res.add(root.val);
        incoder(root.right, res);
    }

    /**
     * 自己实现
     * @param root
     * @return
     */
    public List<Integer> inorderTraversal1(TreeNode root) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        if(root == null)
            return list;
        if(root.left == null && root.right == null){
            list.add(root.val);
            return list;
        }
        if(root.left!=null)
            list.addAll(inorderTraversal(root.left));
        list.add(root.val);
        if(root.right != null)
            list.addAll(inorderTraversal(root.right));
        return list;
    }

    public List<List<Integer>> kNumSum(int[] nums, int k, int m) {
        List<List<Integer>> list = new ArrayList<>();
        for (int i = 0; i < nums.length; i++) {
            int temp = 0;
            for (int j = i; j < i + k; j++) {
                temp += nums[j];
            }
            if (temp == m) {
                List<Integer> list1 = new ArrayList<>();
                for (int j = i; j < i + k; j++) {
                    list1.add(nums[j]);
                }
                list.add(list1);
            }
        }
        return list;
    }


    public static class TreeNode{
        int val;
        TreeNode left;
        TreeNode right;
        TreeNode(){}

        TreeNode(int val){this.val = val;}

        TreeNode(int val, TreeNode left, TreeNode right){
            this.val = val;
            this.left = left;
            this.right = right;
        }
    }

}
