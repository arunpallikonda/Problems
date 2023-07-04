package com.problems.leetcode;

public class RichWealth {
    public static void main(String[] args) {
        //Add individual arrays and give back what is the max wealth amount the 2d array
        int[][] arr = {{1, 2, 3}, {3, 2, 1, 10}};
        System.out.println(maximumWealth(arr));
    }

    public static int maximumWealth(int[][] accounts) {
        int maxWealth = 0;
        int sum;
        for (int[] account : accounts) {
            sum = 0;
            for (int i : account) {
                sum = sum + i;
            }
            if (maxWealth < sum) maxWealth = sum;
        }
        return maxWealth;
    }
}
