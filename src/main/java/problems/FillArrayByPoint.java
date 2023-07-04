package problems;

import java.awt.*;
import java.util.Arrays;
import java.util.Scanner;

public class FillArrayByPoint {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Point[] holdingArray = new Point[20];
        int currentSize = 0;
        boolean stopReceiving = false;
        int i = 0;

        while (currentSize <= 20 && !stopReceiving) {
            System.out.println("Provide new point X value   . END for end");
            if (scanner.hasNextInt()) {
                int x = scanner.nextInt();
                System.out.println("Provide Y value for Point");
                if (scanner.hasNextInt()) {
                    int y = scanner.nextInt();
                    Point point = new Point(x, y);
                    holdingArray[currentSize] = point;
                    currentSize++;
                }
            } else {
                if (currentSize == 0) {
                    System.out.println("Fill atleast one POINT and then call END");
                } else {
                    stopReceiving = scanner.next().equalsIgnoreCase("END");
                }
            }
        }
        System.out.println(Arrays.toString(holdingArray));
    }
}
