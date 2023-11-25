package com.problems.leetcode;

public class Palindrome {

    //number = 0-9 - not palindrome = negative also
    // number > 9 - check
    public static boolean isPalindrome(int number) {
        if (number < 9) return false;
        int forwardNumber = number;
        int reverseNumber = 0;

        while (number != 0) {
            int digit = number % 10;
            reverseNumber = reverseNumber * 10 + digit;
            number = number / 10;
        }

        System.out.println(forwardNumber);
        System.out.println(reverseNumber);

        return forwardNumber == reverseNumber;
    }

    public static void main(String[] args) {
        isPalindrome(561);
    }
}
