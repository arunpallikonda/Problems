package com.problems.leetcode;

import java.util.ArrayList;
import java.util.List;

public class GreatestCandy {
    public static void main(String[] args) {
        //add extra candy to each person and tell if he will be the one with highest candies.
        System.out.println(kidsWithCandies(new int[]{2, 3, 5, 1, 3}, 3));
    }

    public static List<Boolean> kidsWithCandies(int[] candies, int extraCandies) {
        int[] newarr = new int[candies.length];
        int highest = 0;
        for (int i = 0; i < candies.length; i++) {
            if (highest < candies[i]) highest = candies[i];
            newarr[i] = candies[i] + extraCandies;
        }
        List<Boolean> booleans = new ArrayList<>();
        for (int arr : newarr) {
            booleans.add(arr >= highest);
        }
        return booleans;
    }
}
