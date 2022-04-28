import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZhongXuBianLi {
    public static void main(String[] args) {

        int[] data = new int[]{1,2,3,4,5,6,8,10};
        int k = 2;
        int m = 7;

        List<List<Integer>> list = kNumSum(data,k,m,0);
        for (List<Integer> l:list) {
            System.out.println(l.toString());
        }


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

    /**
     * 求一个数组中K个数之和为M的组合：递归 + 双指针（）实现 k>2 时候，递归调用，k=2的时候，作为判断终止条件返回。
     * @param nums
     * @param k
     * @param m
     * @return
     */
    public static List<List<Integer>> kNumSum(int[] nums, int k, int m, int start) {
        List<List<Integer>> res = new ArrayList<>();
        if (start >= nums.length){
            return res;
        }
        if (k==2){
            int l = start, h = nums.length-1;
            while (l<h){
                if (nums[l] + nums[h] == m){
                    List<Integer> list = new ArrayList<>();
                    list.add(nums[l]);
                    list.add(nums[h]);
                    res.add(list);
                    while (l < h && nums[l] == nums[l+1])
                        l++;
                    while (l<h && nums[h] == nums[h-1])
                        h--;
                    l++;
                    h--;
                }else if (nums[l] + nums[h] < m){
                    l++;// 参与计算的值比较小，左边的指针先右移
                }else {
                    h--; // 参与计算的值比较大，右边的指针向左移
                }
            }
        }
        if (k>2){
            for (int i=start;i<nums.length-k+1;i++){
                List<List<Integer>> temp = kNumSum(nums, k-1, m-nums[i], i+1);
                if (temp != null){
                    for (List<Integer> l:temp) {
                        l.add(0,nums[i]);
                    }
                    res.addAll(temp);
                }
                while (i<nums.length-1 && nums[i] == nums[i+1]){
                    i++;
                }
            }
            return res;
        }
        return res;
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
