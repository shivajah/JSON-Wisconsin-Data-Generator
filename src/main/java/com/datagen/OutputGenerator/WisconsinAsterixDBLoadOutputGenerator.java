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

import com.datagen.FieldGenerators.AWisconsinGenerator;
import com.datagen.schema.Schema;
import com.datagen.Server;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;


public class WisconsinAsterixDBLoadOutputGenerator extends AWisconsinOutputGenerator {

    Socket sock;
    OutputStream output = null;
    PrintWriter writer;
    public WisconsinAsterixDBLoadOutputGenerator(Schema schema, List<AWisconsinGenerator> generators) {
        super(schema, generators);
        initiate();
    }

    @Override
    public void write(int threadId, List<String> batchOfRecords, int batchIndex) {
        for (String record : batchOfRecords) {
            writer.write(record);
        }
    }

    @Override
    public void closeWriter(int threadID) {
        try {
            writer.flush();
            writer.close();
            sock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void initiate() {
        super.initiate();
        try {
            sock = new Socket(Server.JSONDataGenConfiguration.get(Server.HOST_NAME_FIELD_NAME),
                    Integer.parseInt(Server.JSONDataGenConfiguration.get(Server.ASTERIXDB_LOAD_PORT)));
            output = sock.getOutputStream();
            writer = new PrintWriter(output, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
