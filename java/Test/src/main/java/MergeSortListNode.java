public class MergeSortListNode {
    public static void main(String[] args) {

    }

    /**
     * 合并两个有序链表：迭代实现
     * 思路：可以使用迭代方法来实现上述算法。 当都不是空链表时，判断哪个链表的头节点的值更小，将较小值的节点添加到结果里，
     * 当一个节点被添加到结果里之后，将对应链表中的节点向后移动一位。
     * 算法：
     * 1、设定一个哨兵节点prehead，为了最后我们比较容易返回合并后的链表，
     * 2、维护一个prev指针，需要做的是调整它的next指针。
     * 3、重复以下过程，直到l1 或者 l2 指向了null: 如果l1的当前节点值小于等于l2，我们就把l1当前节点接在prev
     * 节点后面，同时将l1指针往后移动一位，否则我们对l2做同样的操作。不管我们将哪个元素接在了后面，我们都需要把prev向后移动一位。
     * 4、循环终止的时候，l1和l2至多有一个是非空的，由于输入的两个链表都是有序的，所以不管哪个链表非空，它包含的所有元素都比之前合并
     * 链表中的所有元素都要大。这意味着只需要简单地将非空链表接在合并链表的后面，并返回合并链表即可。
     *
     * 时间复杂度：O（n+m）每次循环迭代中，两个链表中只有一个元素被放进合并链表中，因此while循环的次数不会超过两个链表的长度之和。
     * 所有其他操作的时间复杂度都是常数级别的，因此总的时间复杂度为O(n+m)
     * 空间复杂度：O(1) 只需要常数的空间存放若干变量
     *
     * @param listNode1
     * @param listNode2
     * @return
     */
    public static ListNode mergeTwoLists(ListNode listNode1, ListNode listNode2){
        ListNode preHead = new ListNode(-1);
        ListNode prev = preHead;

        while (listNode1 != null && listNode2 != null){
            if (listNode1.val <= listNode2.val){
                prev.next = listNode1;
                listNode1 = listNode1.next;
            }else {
                prev.next = listNode2;
                listNode2 = listNode2.next;
            }
            prev = prev.next;
        }

        // 合并后listNode1 和 listNode2最多只有一个还未被合并完，我们直接将链表末尾指向合并完的链表即可。
        prev.next = listNode1 == null? listNode2:listNode1;
        return preHead.next;
    }

    public static class ListNode{
        int val;
        ListNode next;
        ListNode(){}
        ListNode(int val){
            this.val = val;
        }
        ListNode(int val, ListNode next) {
            this.val = val;
            this.next = next;
        }

    }
}
