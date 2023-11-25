package com.problems.learning;

import java.util.Arrays;
import java.util.Scanner;

public class FillArrayByInputInteger {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int[] holdingArray = new int[20];
        int currentSize = 0;
        boolean stopReceiving = false;
        int i = 0;
        while (currentSize <= 20 && !stopReceiving) {
            System.out.println("Enter integers to fill. Maximum allowed is 20, currentSize: " + currentSize + ", Use END when done.");

            if (scanner.hasNextInt()) {
                i = scanner.nextInt();
                holdingArray[currentSize] = i;
                currentSize++;
            } else {
                if (currentSize == 0) {
                    System.out.println("Input at least one integer and then END");
                } else {
                    stopReceiving = scanner.next().equalsIgnoreCase("END");
                }
            }
        }
        System.out.println(Arrays.toString(holdingArray));
    }
}
