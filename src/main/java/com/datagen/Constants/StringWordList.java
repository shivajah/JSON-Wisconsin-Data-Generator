/*Copyright (c) 2020 Shiva Jahangiri

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
        SOFTWARE.
*/
package com.datagen.Constants;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.datagen.Server;
import org.apache.commons.io.FileUtils;

public class StringWordList {

    private static List<String> wordList;
    private static StringWordList instance;

    StringWordList() throws IOException {
        wordList = FileUtils.readLines(new File(Server.workloadsFolder+"wordlist"), "utf-8");
    }

    public static StringWordList getInstance() throws IOException {
        if (instance == null) {
            synchronized (StringWordList.class) {
                if (instance == null) {
                    instance = new StringWordList();
                }
            }
        }
        return instance;
    }

    public List<String> getWordList() {
        return wordList;
    }

}
