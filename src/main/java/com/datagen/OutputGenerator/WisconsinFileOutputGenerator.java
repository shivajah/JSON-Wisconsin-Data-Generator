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
package com.datagen.OutputGenerator;

import com.datagen.FieldGenerators.WisconsinGenerator;
import com.datagen.Schema.Schema;
import com.datagen.Server;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

public class WisconsinFileOutputGenerator extends AWisconsinOutputGenerator {

    public WisconsinFileOutputGenerator(Schema schema, List<WisconsinGenerator> generators) {
        super(schema, generators);
        initiate();
    }

    @Override
    public void write(int threadId, List<String> batchOfRecords, int batchIndex) {
        for (String record : batchOfRecords) {
            try {
                threadToFileOutputStream.get(threadId).write(record.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void closeWriter(int threadId) {
        try {
            threadToFileOutputStream.get(threadId).close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void initiate() {
        super.initiate();
        //TODO: use schema file name for this purpose
        String filePrefix = Server.couchbaseConfiguration.get(Server.FILEOUTPUT_NAME).split(".adm")[0];
        final String fileName = (filePrefix != null) ? directory + filePrefix : directory + "tempFile";
        File f = new File(fileName);
        IntStream.range(0, Integer.valueOf(Server.couchbaseConfiguration.get(Server.PARTITIONS_NAME))).forEach(readerId -> {
            String execFilename = fileName + "p_" + readerId + ".adm";
            File execFile = new File(execFilename);
            try {
                execFile.createNewFile();
                threadToFileOutputStream.put(readerId, new BufferedOutputStream(new FileOutputStream(execFilename)));
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }
}
