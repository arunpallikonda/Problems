package com.problems.learning;

import java.util.Arrays;

public class OddBeforeEvenArray {
    public static void main(String[] args) {
        int[] arr = {1, 3, 19, 9, 8, 2, 6, 17};
        int[] temp = new int[arr.length];
        int tempStartIndex = 0;
        int tempLastIndex = arr.length-1;

        for (int i = 0; i < arr.length; i++) {
            if (arr[i] % 2 == 1) {
                temp[tempStartIndex++] = arr[i];
            } else {
                temp[tempLastIndex--] = arr[i];
            }
        }

        System.out.println(Arrays.toString(temp));
    }
}
