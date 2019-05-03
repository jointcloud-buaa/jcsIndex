package edu.buaa.act.jcsindex.local.jcssti.index.grid;

/**
 * Created by shmin at 2018/5/25 7:28
 **/
public class ZOrder {
    public static Integer getZOrderStr(Integer x, Integer y) {
        String xBinStr = Integer.toBinaryString(x);
        String yBinStr = Integer.toBinaryString(y);

        int maxLen = xBinStr.length();
        int minLen = yBinStr.length();
        if (maxLen < minLen) {
            int tmp = maxLen;
            maxLen = minLen;
            minLen = tmp;
        }

        char[] resCharArr = new char[maxLen * 2];

        int xi = 0, yi = 0, k = 0;
        if (xBinStr.length() > yBinStr.length()) {
            for (; xi < xBinStr.length() - yBinStr.length(); xi++) {
                resCharArr[k++] = xBinStr.charAt(xi);
                resCharArr[k++] = '0';
            }
        } else if (xBinStr.length() < yBinStr.length()) {
            for (; yi < yBinStr.length() - xBinStr.length(); yi++) {
                resCharArr[k++] = '0';
                resCharArr[k++] = yBinStr.charAt(yi);
            }
        }
        for (; xi != xBinStr.length(); xi++, yi++) {
            resCharArr[k++] = xBinStr.charAt(xi);
            resCharArr[k++] = yBinStr.charAt(yi);
        }

        Integer res = (Integer.parseInt(new String(resCharArr), 2));
        return res;
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        // 在(0,0)(2,2)组成的矩形内的点的zOrder值都比这两个点的zOrder值范围内，但(1,3)不在(0,0)(2,2)组成的矩形内，说明有false positive
        System.out.println(getZOrderStr(0, 0));
        System.out.println(getZOrderStr(2, 2));
        System.out.println(getZOrderStr(1, 3));

        System.out.println(getZOrderStr(0B111_1111_1111_1111, 0B111_1111_1111_1111));
        System.out.println(getZOrderStr(18000, 18000));

        System.out.println();

        for (int i = 3; i >= 0; i--) {
            for (int j = 0; j < 4; j++) {
                System.out.printf("%5d ", getZOrderStr(j, i));
            }
            System.out.println();
        }
        System.out.println(getZOrderStr(999, 999));
    }
}

