package com.problems.leetcode;

public class StackQueue {

    class Stack {
        int[] stackArray;
        int top;
        int maxSize;

        public Stack(int maxSize) {
            this.maxSize = maxSize;
            stackArray = new int[maxSize];
            top = -1;
        }

        public void push(int item) {
            if (top == maxSize - 1) {
                System.out.println("Not Possible");
            } else {
                stackArray[++top] = item;
            }
        }

        public int pop() {
            if (top == -1) {
                System.out.println("Not Possible");
                return -1;
            } else {
                return stackArray[top--];
            }
        }

        public boolean isEmpty() {
            return top == -1;
        }

        public int size() {
            return top + 1;
        }
    }

    class Queue{
        int[] queueArray;
        int front;
        int back;
        int maxSize;
        int currentSize;
    }
}
