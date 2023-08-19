package com.problems.leetcode;

public class DoublyLinkedList {

    class Node {
        int data;
        Node prev;
        Node next;

        public Node(int data) {
            this.data = data;
            prev = null;
            next = null;
        }
    }

    class DLinkedList {
        Node head;
        Node tail;

//        | |
//       newItem |item = head = tail|
//        |head = item | tail = item1|
//        |head = item | item1| tail = item2
        public void insertAtStart(int data) {
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
//         |item = head = tail| newItem
//        |head = item | tail = item1|
//        |head = item | item1| tail = item2
        public void insertAtEnd(int data) {
            Node node = new Node(data);
            if (tail == null) {
                tail = node;
                head = node;
            } else {
                node.prev = tail;
                tail.next = node;
                tail = node;
            }
        }

//        | |
//        |item = head = tail|
//        |head = item | tail = item1|
        public void delete(int data) {
            Node currentNode = tail;

            while (currentNode != null) {
                if (currentNode.data == data) {
                    if (currentNode == tail) {
                        tail = tail.prev;
                        if (tail == null) {
                            head = null;
                        } else {
                            tail.next = null;
                        }
                    } else if (currentNode == head) {
                        head = head.next;
                        if (head == null) {
                            tail = null;
                        } else {
                            head.prev = null;
                        }
                    } else {
                        currentNode.prev.next = currentNode.next;
                        currentNode.next.prev = currentNode.prev;
                    }
                    break;
                } else {
                    currentNode = tail.prev;
                }
            }
        }
    }
}
