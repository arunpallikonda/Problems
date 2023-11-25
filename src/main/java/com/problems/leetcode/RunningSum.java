package com.problems.leetcode;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RunningSum {
    public static void main(String[] args) {
// need to return array adding previous plus current like for below {1,3,6,10}
        System.out.println((Arrays.toString(runningSum2(new int[]{1, 2, 3, 4}))));
    }

    public static int[] runningSum(int[] nums) {
        final AtomicInteger sum = new AtomicInteger(0);
        List<Integer> test = new ArrayList<>();
        Arrays.stream(nums).forEach(x -> test.add(sum.addAndGet(x)));
        return test.stream().mapToInt(i -> i).toArray();
    }

    public static int[] runningSum2(int[] nums) {
        int[] arr = new int[nums.length];
        int sum = 0;
        for (int i = 0; i < nums.length; i++) {
            sum = sum + nums[i];
            arr[i] = sum;
        }
        return arr;
    }

}