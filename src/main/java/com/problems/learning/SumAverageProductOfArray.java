package com.problems.learning;

public class SumAverageProductOfArray {

    public static void main(String[] args) {
        int[] arr = {1, 5, 6, 9, 4};
        int sum = 0;
        int product = 1;

        for (int j : arr) {
            sum += j;
            product = product * j;
        }
        System.out.println("sum: " + sum);
        System.out.println("product: " + product);
        System.out.println("average: " + sum / arr.length);
    }
}
