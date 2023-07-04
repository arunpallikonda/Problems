package com.problems.leetcode;

public class DoublyLinkedListDeleteIndex {
    static class Node {
        int data;
        Node prev;
        Node next;

        public Node(int data) {
            this.data = data;
            prev = null;
            next = null;
        }
    }

    static class DLList {
        Node head;
        Node tail;


        public void addAtStart(int data) {
            Node node = new Node(data);
            if (head == null) {
                head = node;
                tail = node;
            } else {
                node.next = head;
                head.prev = node;
                head = node;
            }
        }

//        | |
//                |head=item=tail|
//                |head =item|tail =item1

        public void addAtEnd(int data) {
            Node node = new Node(data);
            if (tail == null) {
                head = node;
                tail=node;
            } else {
                node.prev = tail;
                tail.next = node;
            }
            tail = node;
        }

//        | |
//                |head =item|

        public void delete(int data) {
            Node currentNode = head;

            while (currentNode != null) {
                if (currentNode.data == data) {
                    if (currentNode == head) {
                        head = head.next;
                        if (head == null) {
                            tail = null;
                        } else {
                            head.prev = null;
                        }
                    }
                    break;
                } else {
                    currentNode = currentNode.next;
                }
            }
        }

        public void deleteIndex(int index) {
            if (index < 0) {
                return;
            }

            Node currentNode = head;
            int currentIndex = 0;

            while (currentIndex < index) {
                if (currentNode != null) {
                    currentNode = currentNode.next;
                    currentIndex += 1;
                } else {
                    return;
                }
            }

            if (currentNode == head) {
                head = head.next;
                if (head == null) {
                    tail = null;
                } else {
                    head.prev = null;
                }
            } else if (currentNode == tail) {
                tail = tail.prev;
                if (tail == null) {
                    head = null;
                } else {
                    tail.next = null;
                }
            } else {
                currentNode.prev.next = currentNode.next;
                currentNode.next.prev = currentNode.prev;
            }
        }
    }

    public static void main(String[] args) {
        DLList list = new DLList();
        list.addAtStart(1);
        list.addAtEnd(2);
//        list.deleteIndex(0);
        list.deleteIndex(1);
        System.out.println(list);
    }
}
