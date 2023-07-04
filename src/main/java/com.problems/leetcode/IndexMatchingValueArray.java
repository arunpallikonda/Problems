package com.problems.leetcode;

public class IndexMatchingValueArray {
    public static int findFirstIndexMatch(int[] nums) {
        int leftIndex = 0;
        int rightIndex = nums.length - 1;

        while (leftIndex < rightIndex) {
            int mid = (leftIndex + rightIndex) / 2;

            if (nums[mid] == mid) {
                //found, check if before matches
                if (mid == 0 || nums[mid - 1] != mid - 1) {
                    //no chance of further match on left
                    return mid;
                } else {
                    rightIndex = mid - 1;
                }
            } else if (nums[mid] < mid) {
                leftIndex = mid + 1;
            } else if (nums[mid] > mid) {
                rightIndex = rightIndex - 1;
            }
        }
        return -1;
    }

    public static void main(String[] args) {
        int[] nums = {-1, 0, 1, 3, 7, 9};

        int result = findFirstIndexMatch(nums);
        if (result != -1) {
            System.out.println("The first index where index = value is: " + result);
        } else {
            System.out.println("No index matches the value at that index.");
        }
    }

}
