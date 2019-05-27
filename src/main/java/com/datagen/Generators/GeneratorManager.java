package com.datagen.Generators;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.datagen.Constants.DataTypes;
import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Output.OutputGenerator;

/**
 * Created by shiva on 2/24/18.
 */
public class GeneratorManager {
    private Map<String, List<String>> parameters;
    private int rowNumbers;
    private Random ran;

    public GeneratorManager(Map<String, List<String>> parameters) {
        this.parameters = parameters;
        ran = new Random();
    }

    public void generateRows() {
        if (!parameters.isEmpty()) {
            String rows = parameters.get("noRows").get(0);
            rowNumbers = (rows == null) ? 1000 : Integer.valueOf(rows);
            OutputGenerator output = new OutputGenerator(parameters);
            List<IGenerator> generators = getGenerators();
            for (int j = 0; j < rowNumbers; j++) {
                String st = "{";
                for (int i = 0; i < generators.size(); i++) {
                    String value;
                    if (i > 0) {
                        st += ", ";
                    }
                    IGenerator generator = generators.get(i);
                    if (generator.getDataType() == DataType.STRING) {
                        int k = ran.nextInt() & Integer.MAX_VALUE;
                        value = "\"" + ((StringGenerator) generator).next(k) + "\"";
                    } else if (generator.getDataType() == DataType.INTEGER) {
                        value = String.valueOf(((NumberGenerator) generator).next());
                    } else if (generator.getDataType() == DataType.BINARY) {
                        int k = ran.nextInt() & Integer.MAX_VALUE;
                        value = "\"" + ((BinaryGenerator) generator).next(k) + "\"";
                    } else {
                        // Default type
                        value = "";
                    }
                    st += "\"" + parameters.get("id").get(i) + "\"" + " : " + value;
                }
                st += "}\n";
                output.write(st);
            }
            output.close();
        }

    }

    private List<IGenerator> getGenerators() {
        List<IGenerator> generators = new LinkedList<>();
        List<String> types = parameters.get("type");
        int index = 0;
        for (String type : types) {
            DataTypes.DataType t = DataTypes.DataType.valueOf(type);
            IGenerator generator = null;
            switch (t) {
                case STRING:
                    generator = new StringGenerator();
                    break;
                case INTEGER:
                    generator = new NumberGenerator(parameters, index);
                    break;
                case BINARY:
                    generator = new BinaryGenerator();
                    break;
                default:
                    System.out.println("Under Construction!");
                    break;
            }
            if (generator != null) {
                generators.add(generator);
            }
            index++;
        }

        return generators;
    }
}
