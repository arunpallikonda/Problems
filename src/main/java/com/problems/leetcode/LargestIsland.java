package com.problems.leetcode;

public class LargestIsland {

    public static int getLargestIsland(int[][] grid) {
        int max = 0;

        for (int i = 0; i < grid.length; i++) {
            for (int j = 0; j < grid[i].length; j++) {
                if (grid[i][j] == 1) {
                    max = Math.max(max, dfs(grid, i, j));
                }
            }
        }
        return max;
    }

    public static int dfs(int[][] grid, int i, int j) {
        if (i < 0 || i >= grid.length || j < 0 || j >= grid[i].length || grid[i][j] != 1) {
            return 0;
        }

        grid[i][j] = 0;
        int count = 1;

        count += dfs(grid, i + 1, j);
        count += dfs(grid, i - 1, j);
        count += dfs(grid, i, j + 1);
        count += dfs(grid, i, j - 1);
        return count;

    }

    public static void main(String[] args) {
        int[][] grid = {{1, 1, 1}, {1, 1, 1}};
        System.out.println(getLargestIsland(grid));
    }
}
