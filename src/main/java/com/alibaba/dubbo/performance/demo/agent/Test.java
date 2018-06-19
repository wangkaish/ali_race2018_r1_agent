/*
 * Copyright 2015-2017 GenerallyCloud.com
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.performance.demo.agent;

import java.io.File;
import java.util.List;

import com.generallycloud.baseio.common.FileUtil;

/**
 * @author wangkai
 *
 */
public class Test {

    public static void main(String[] args) throws Exception {

        printLog("C:/Users/wangkai/Downloads/logs");
//        printLog("C:/Users/wangkai/Desktop/logs");

    }

    static void printLog(String home) throws Exception {

        double large = getCount(home + "/provider-large/logs/provider.log");
        double medium = getCount(home + "/provider-medium/logs/provider.log");
        double small = getCountSmall(home + "/provider-small/logs/provider.log");

        double all = large + medium + small;

        System.out.println("all:" + all);
        System.out.println("large:" + (large));
        System.out.println("medium:" + (medium));
        System.out.println("small:" + (small));
        System.out.println("large:" + (large / all));
        System.out.println("medium:" + (medium / all));
        System.out.println("small:" + (small / all));

    }

    static double getCount(String file) throws Exception {
        List<String> ls = FileUtil.readLines(new File(file));
        String end = ls.get(ls.size() - 1);
        String time = end.substring(0, 19);
        String startTime = getStartTime(time.toCharArray(), 1);
        for (int i = 0; i < ls.size(); i++) {
            if (ls.get(i).startsWith(startTime)) {
                return ls.size() - i;
            }
        }
        return -1;
    }

    static double getCountSmall(String file) throws Exception {
        List<String> ls = FileUtil.readLines(new File(file));
        String end = ls.get(ls.size() - 1);
        String time = end.substring(0, 19);
        String startTime1 = getStartTime(time.toCharArray(), 1);
        String startTime2 = getStartTimeSmall(startTime1.toCharArray(), 1);
        String startTime3 = getStartTimeSmall(startTime1.toCharArray(), 2);
        for (int i = 0; i < ls.size(); i++) {
            if (ls.get(i).startsWith(startTime1) || ls.get(i).startsWith(startTime2)
                    || ls.get(i).startsWith(startTime3)) {
                return ls.size() - i;
            }
        }
        return -1;
    }

    static String getStartTime(char[] arr, int j) {
        int n1 = Integer.parseInt(new String(new char[] { arr[15] }));
        n1 -= j;
        if (n1 < 0) {
            n1 += 10;
            int n2 = Integer.parseInt(new String(new char[] { arr[14] }));
            n2--;
            arr[14] = String.valueOf(n2).charAt(0);
            arr[15] = String.valueOf(n1).charAt(0);
        } else {
            arr[15] = String.valueOf(n1).charAt(0);
        }
        return new String(arr);
    }

    static String getStartTimeSmall(char[] arr, int j) {
        int n1 = Integer.parseInt(new String(new char[] { arr[18] }));
        n1 += j;
        if (n1 > 10) {
            n1 -= 10;
            int n2 = Integer.parseInt(new String(new char[] { arr[17] }));
            n2++;
            arr[17] = String.valueOf(n2).charAt(0);
            arr[18] = String.valueOf(n1).charAt(0);
        } else {
            arr[18] = String.valueOf(n1).charAt(0);
        }
        return new String(arr);
    }

}
