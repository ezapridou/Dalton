/*Copyright (c) 2022 Data Intensive Applications and Systems Laboratory (DIAS)
                   Ecole Polytechnique Federale de Lausanne
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

package correlation_clustering.stringsimilarity;

import java.io.Serializable;

import record.*;

/**
 * Dynamic programming approach to compute edit distance
 * see https://www.geeksforgeeks.org/edit-distance-dp-5/
 */
public class Similarity implements Serializable {
    private int dp[][];
    private final int MAX_STRING_SIZE = 50;

    public Similarity() {
        dp = new int[MAX_STRING_SIZE][MAX_STRING_SIZE];
        // first row and first row are always the same
        for (int i = 0; i < MAX_STRING_SIZE; i++) {
            dp[i][0] = i;
            dp[0][i] = i;
        }
    }

    public int getMaxStrSize(){
        return MAX_STRING_SIZE;
    }

    public int findDistStrs(String str1, String str2) {
        int len1 = str1.length();
        int len2 = str2.length();

        for (int i = 1; i <= len1; i++) {
            for (int j = 1; j <= len2; j++) {
                if (str1.charAt(i - 1) == str2.charAt(j - 1))
                    dp[i][j] = dp[i - 1][j - 1];
                    // If the last character is different, consider all possibilities and find the minimum
                else
                    dp[i][j] = 1 + Math.min(Math.min(dp[i][j - 1], // Insert
                            dp[i - 1][j]), // Remove
                            dp[i - 1][j - 1]); // Replace
            }
        }
        return dp[len1][len2];
    }

    // Check if all the attributes are similar (edit distance > threshold)
    public int findDist(RecordStr t1, RecordStr t2) {
        int maxDist = 0;
        int dist = 0;
        dist = findDistStrs(t1.getStr(), t2.getStr());
        if (dist > maxDist){
            maxDist = dist;
        }
        return dist;
    }

    // translate edit distance to similarity
    public double similarity(String s1, String s2){
        double ed = findDistStrs(s1, s2) * 1.0;
        int maxSize;
        if (s1.length() > s2.length()){
            maxSize = s1.length();
        }
        else{
            maxSize = s2.length();
        }
        return 1 - (ed / (double)maxSize); //transform edit distance to similarity
    }

}