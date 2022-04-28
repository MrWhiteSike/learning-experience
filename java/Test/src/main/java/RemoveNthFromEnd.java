import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RemoveNthFromEnd {

    public static void main(String[] args) {
        ListNode head = new ListNode(1);
        ListNode first = head;
        int len = 2;
        while (len < 6){
            first.next = new ListNode(len++);
            first = first.next;
        }

        ListNode listNode = removeNthFromEnd(head, 2);
        while (listNode != null){
            System.out.println(listNode.val);
            listNode = listNode.next;
        }
    }

    public static ListNode removeNthFromEnd(ListNode head, int n) {
        ListNode result = head;
        ListNode first = head;
        int len = 0;
        while (head.next != null){
            len++;
            head = head.next;
        }

        while (true){
            if (len-n == 0){
                first.next = first.next.next;
                break;
            }
            len--;
            first = first.next;
        }
        return result;
    }

    public static ListNode removeNthFromEnd2(ListNode head, int n) {
        ListNode first = head;
        List<ListNode> list = new ArrayList<>();
        while (head != null){
            list.add(head);
            head = head.next;
        }
        if (list.size() < n){
            return first;
        }
        list.remove(list.size()-n);
        for (int i=0;i<list.size()-1;i++) {
            list.get(i).next = list.get(i + 1);
        }
        return list.get(0);
    }

    public ListNode mergeTwoLists(ListNode list1, ListNode list2) {
        List<ListNode> list11= new ArrayList<>();
        List<ListNode> list12= new ArrayList<>();
        while (list1!=null){
            list11.add(list1);
            list1 = list1.next;
        }
        while (list2 != null){
            list12.add(list2);
            list2 = list2.next;
        }
        list11.addAll(list12);
        if (list11.size() == 0){
            return null;
        }
        list11.sort(new Comparator<ListNode>() {
            @Override
            public int compare(ListNode o1, ListNode o2) {
                return o1.val - o2.val;
            }
        });
        for (int i=0;i<list11.size()-1;i++){
            list11.get(i).next = list11.get(i+1);
        }
        return list11.get(0);
    }



    public static class ListNode {
        int val;
        ListNode next;
        ListNode() {}
        ListNode(int val) { this.val = val; }
        ListNode(int val, ListNode next) { this.val = val; this.next = next; }
    }

}
