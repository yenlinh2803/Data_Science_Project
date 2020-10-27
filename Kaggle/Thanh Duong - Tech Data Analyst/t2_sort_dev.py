#!/usr/bin/env python3
# File name: t2_sort_dev.py
# Description: The Python scripts for test the sort algorithms
# Author: Thanh Duong
# Date: 28-07-2020

import timeit


def bubble_sort(l):
    for i in range(len(l)):
        for j in range(len(l)-i-1):
            if l[j+1] < l[j]:
                l[j], l[j+1] = l[j+1], l[j]


def insertion_sort(l):
    for i in range(len(l)):
        try:
            flag = l[i+1]
            while i >= 0 and flag < l[i]:
                l[i + 1] = l[i]
                i -= 1
            l[i + 1] = flag
        except IndexError:
            pass


def main():
    try:
        input_arr = input("Enter a list of number, ex. 1,3,2 : ")
        tmp = eval('['+input_arr+']')
        start = timeit.default_timer()
        bubble_sort(tmp)
        end = timeit.default_timer()
        print('''
        * Bubble Sort
        Time processed: {} (s)
        Time complexity: 
        - Best: O(N)
        - Avg.: O(N^2)
        - Worst: O(N^2)
        '''.format(end-start))
        print(tmp)

        tmp = eval('['+input_arr+']')
        start = timeit.default_timer()
        insertion_sort(tmp)
        end = timeit.default_timer()
        print('''
        * Insertion Sort
        Time processed: {} (s)
        Time complexity: 
        - Best: O(N)
        - Avg.: O(N^2)
        - Worst: O(N^2)
        '''.format(end-start))
        print(tmp)
    except:
        print('Please re-run this script and enter the correct format!')


if __name__ == "__main__":
    main()
