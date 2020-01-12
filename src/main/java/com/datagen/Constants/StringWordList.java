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
