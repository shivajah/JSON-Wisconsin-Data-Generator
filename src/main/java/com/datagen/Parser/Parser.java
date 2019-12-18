package com.datagen.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datagen.Constants.DataTypes;
import com.datagen.Constants.Order;
import com.datagen.schema.Field;
import com.datagen.schema.Schema;
import com.datagen.schema.Domain.Domain;
import com.datagen.schema.Domain.IntegerDomain;
import com.datagen.schema.Domain.Range;
import com.datagen.schema.Domain.StringDomain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;


/**
 * Created by shiva on 3/1/18.
 */
public class Parser {
    public List<Schema> parseWisconsinConfigFile(String configFile) {
        List<SchemaForParser> schemas = new LinkedList<>();
        List<Schema> finalSchemas = new LinkedList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        File file = new File(configFile);
        try {
            schemas = objectMapper.readValue(file, new TypeReference<List<SchemaForParser>>() {
            });
        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }
        for (SchemaForParser sp : schemas) {
            finalSchemas.add(generateSchema(sp));
        }
        return finalSchemas;
    }

    private Schema generateSchema(SchemaForParser sp) {
        Schema schema = new Schema();
        List<Field> fields = new LinkedList<>();
        schema.setCardinality(sp.dataset.cardinality);
        schema.setDatasetName(sp.dataset.name);
        schema.setFileName(sp.file.name);
        schema.setNumOfPartitions(sp.file.partitions);
        schema.setFileSize(sp.dataset.fileSize);
        Field field;
        for (PField f : sp.fields) {
            field = new Field();
            field.setName(f.name);
            field.setDeclared(f.declared);
            field.setNullable(f.nullable);
            field.setNulls(f.nulls);
            field.setOptional(f.optional);
            field.setMissings(f.missings);
            field.setRange(f.range);
            field.setOdd(f.odd);
            field.setEven(f.even);
            field.setStringLength(f.length);
            field.setVariableLength(f.variableLength);
            field.setWord(f.word);
            field.setNormalDistribution(f.normalDistribution);
            field.setStandardDeviation(f.standardDeviation);
            field.setShape(f.shape);
            field.setScale(f.scale);
            field.setBernoulliDistribution(f.bernoulliDistribution);
            field.setProbLargeRecord(f.probLargeRecord);
            field.setMinSizeSmall(f.minSizeSmall);
            field.setMaxSizeSmall(f.maxSizeSmall);
            field.setMinSizeLarge(f.minSizeLarge);
            field.setMaxSizeLarge(f.maxSizeLarge);
            Order.order order = (f.order.equalsIgnoreCase("random") ? Order.order.RANDOM
                    : (f.order.equalsIgnoreCase("sequential") ? Order.order.SEQUENTIAL : null));
            field.setOrder(order);
            DataTypes.DataType dataType;

            if (f.type.equalsIgnoreCase("string")) {
                dataType = DataTypes.DataType.STRING;
            } else if (f.type.equalsIgnoreCase("integer")) {
                dataType = DataTypes.DataType.INTEGER;
            } else if (f.type.equalsIgnoreCase("binary")) {
                dataType = DataTypes.DataType.BINARY;
            } else {
                dataType = null;
            }

            //            DataTypes.DataType dataType = (f.type.equalsIgnoreCase("string") ? DataTypes.DataType.STRING
            //                    : (f.type.equalsIgnoreCase("integer") ? DataTypes.DataType.INTEGER : null));

            field.setType(dataType);
            Map<String, Domain> domains = parseDomain();
            if (domains.get(f.domain) != null) {
                field.setDomain(domains.get(f.domain));
            }
            fields.add(field);
        }
        schema.setFields(fields);
        setPrimeAndGenerator(schema);
        return schema;
    }

    //    public Schema parseConfigFile(){
    //        Schema schema  =  new Schema();
    //        Map<String, Domain> domains = parseDomain();
    //        try {
    //            File inputFile = new File("./src/configs/shivsconfig");
    //            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    //            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    //            Document doc =  dBuilder.parse(inputFile);
    //            doc.getDocumentElement().normalize();
    //            //get cardinality
    //            NodeList nList = doc.getElementsByTagName("dataset");
    //            Node nNode = nList.item(0);
    //            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //                Element element = (Element) nNode;
    //                schema.setCardinality(Integer.parseInt(getElement(element,"cardinality",0)));
    //                schema.setDatasetName(getElement(element,"name",0));
    //            }
    //            setPrimeAndGenerator(schema);
    //            //get file info
    //            nList = doc.getElementsByTagName("file");
    //            nNode = nList.item(0);
    //            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //                Element element = (Element) nNode;
    //                schema.setFileName(getElement(element,"name",0));
    //                schema.setNumOfPartitions(Integer.parseInt(getElement(element,"partitions",0)));
    //            }
    //            //getFields
    //             nList = doc.getElementsByTagName("field");
    //            Field field;
    //            for (int temp = 0; temp < nList.getLength(); temp++) {
    //                 nNode = nList.item(temp);
    //                field = new Field();
    //                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //                    Element eElement = (Element) nNode;
    //                    field.setPosition(Integer.parseInt(eElement.getAttribute("position")));
    //                    field.setName(getElement(eElement,"name",0));
    //                    String t = getElement(eElement,"type",0);
    //                    DataTypes.DataType type = t.equalsIgnoreCase("string")? DataTypes.DataType.STRING :(t.equalsIgnoreCase("integer")?
    //                            DataTypes.DataType.INTEGER: null);
    //                    field.setType(type);
    //                    field.setDeclared(getElement(eElement,"declared",0).equalsIgnoreCase("true"));
    //                    String domainst = getElement(eElement,"domain",0);
    //                    if(!domains.containsKey(domainst)){
    //                        throw new IllegalArgumentException("Invalid domain name has been entered.");
    //                    }
    //                    field.setDomain(domains.get(domainst));
    //                    String orderst =  getElement(eElement,"order",0);
    //                    Order.order order = orderst.equalsIgnoreCase("random")? Order.order.RANDOM:(orderst.equalsIgnoreCase("sequential")?
    //                            Order.order.SEQUENTIAL:null);
    //                    field.setOrder(order);
    ////                    if(!schema.getFields().isEmpty()&&schema.getFields().get(field.getPosition())!= null){
    ////                        throw new IllegalArgumentException("Multiple fields have been selected for the same position in the schema.");
    ////                    }
    //                    schema.getFields().add(field);
    //                }
    //            }
    //        } catch (Exception e) {
    //            e.printStackTrace();
    //        }
    //
    //        return schema;
    //    }
    //public List<Schema> parseWisconsinConfigFile(){
    //        List<Schema> schemas = new LinkedList<>();
    //    Schema schema;
    //    Map<String, Domain> domains = parseDomain();
    //    try {
    //        File inputFile = new File("./src/configs/wconfig");
    //        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    //        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    //        Document doc =  dBuilder.parse(inputFile);
    //        doc.getDocumentElement().normalize();
    //        //get cardinality
    //        NodeList nodeList = doc.getElementsByTagName("info");
    //        for(int i=0;i<nodeList.getLength();i++) {
    //            schema = new Schema();
    //            NodeList nList = doc.getElementsByTagName("dataset");
    //            Node nNode = nList.item(i);
    //            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //                Element element = (Element) nNode;
    //                schema.setCardinality(Integer.parseInt(getElement(element, "cardinality",i)));
    //                schema.setDatasetName(getElement(element, "name",i));
    //            }
    //            setPrimeAndGenerator(schema);
    //            //get file info
    //            nList = doc.getElementsByTagName("file");
    //            nNode = nList.item(i);
    //            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //                Element element = (Element) nNode;
    //                schema.setFileName(getElement(element, "name",i));
    //                schema.setNumOfPartitions(Integer.parseInt(getElement(element, "partitions",i)));
    //            }
    //            //getFields
    //            nList = doc.getElementsByTagName("field");
    //            Field field;
    //            for (int temp = 0; temp < nList.getLength(); temp++) {
    //                nNode = nList.item(temp);
    //                field = new Field();
    //                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //                    Element eElement = (Element) nNode;
    //                    field.setName(getElement(eElement, "name",0));
    //                    String t = getElement(eElement, "type",0);
    //                    DataTypes.DataType type = t.equalsIgnoreCase("string") ?
    //                            DataTypes.DataType.STRING :
    //                            (t.equalsIgnoreCase("integer") ? DataTypes.DataType.INTEGER : null);
    //                    field.setType(type);
    //                    if (field.getType() == DataTypes.DataType.STRING) {
    //                        String length = getElement(eElement, "length",0);
    //                        int len = 0;
    //                        if (length != null) {
    //                            len = Integer.parseInt(length);
    //                        }
    //                        field.setStringLength(len);
    //                    }
    //                    String range = getElement(eElement, "range",0);
    //                    int rangeInt = 0;
    //                    if (range != null) {
    //                        rangeInt = Integer.parseInt(range);
    //                    }
    //                    field.setRange(rangeInt);
    //                    field.setDeclared(getElement(eElement, "declared",0).equalsIgnoreCase("true"));
    //                    String domainst = getElement(eElement, "domain",0);
    //                    if (domainst != null && !domains.containsKey(domainst)) {
    //                        throw new IllegalArgumentException("Invalid domain name has been entered.");
    //                    }
    //                    field.setDomain(domains.get(domainst));
    //                    String orderst = getElement(eElement, "order",0);
    //                    Order.order order = orderst.equalsIgnoreCase("random") ?
    //                            Order.order.RANDOM :
    //                            (orderst.equalsIgnoreCase("sequential") ? Order.order.SEQUENTIAL : null);
    //                    field.setOrder(order);
    //                    schema.getFields().add(field);
    //                }
    //            }
    //            schemas.add(schema);
    //        }
    //    } catch (Exception e) {
    //        e.printStackTrace();
    //    }
    //
    //    return schemas;
    //}
    public Map<String, Domain> parseDomain() {
        Map<String, Domain> domains = new HashMap<String, Domain>();
        try {
            File inputFile = new File("./configs/domain");
            FileReader fileReader = new FileReader(inputFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                Domain domain = parse(line);
                domains.put(domain.getName(), domain);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return domains;
    }

    private Domain parse(String line) throws IllegalArgumentException {
        Domain domain;
        String[] colonSignSplits = line.split(":");
        String type = colonSignSplits[0].trim();
        if (type.equalsIgnoreCase("String")) {
            domain = new StringDomain();
        } else if (type.equalsIgnoreCase("Integer")) {
            domain = new IntegerDomain();
        } else {
            throw new IllegalArgumentException();
        }
        String name = colonSignSplits[1].trim();
        String[] equalSignSplits = name.split("=");
        domain.setName(equalSignSplits[0].trim());
        Domain.Type rangeOrVal;
        char rangeOrValue = equalSignSplits[1].trim().charAt(0);
        rangeOrVal = rangeOrValue == '<' ? Domain.Type.RANGE : (rangeOrValue == '{' ? Domain.Type.VALUE : null);
        String values = equalSignSplits[1].trim();
        values = values.substring(1, values.length() - 1);
        domain.setType(rangeOrVal);
        setValues(domain, values);
        return domain;
    }

    private void setValues(Domain domain, String values) {
        String[] splits = values.split(",");
        if (domain.getType() == Domain.Type.RANGE) {
            Range range = new Range();
            range.setFrom(splits[0]);
            range.setTo(splits[1]);
            domain.setRange(range);
        } else if (domain.getType() == Domain.Type.VALUE) {
            List<String> vals = new LinkedList<>();
            for (String s : splits) {
                vals.add(s);
            }
            domain.setValues(vals);
        } else {
            throw new IllegalArgumentException();
        }
    }
    //    private String getElement(Element e, String tag,int pos){
    //        NodeList nl= e.getElementsByTagName(tag);
    //        if(nl.getLength() >0 && nl.getLength() >= pos){
    //            return nl.item(pos).getTextContent();
    //        }
    //        return null;
    //    }

    private void setPrimeAndGenerator(Schema schema) {
        long cardinality = schema.getCardinality();
        long generator;
        long prime;
        if (cardinality <= Math.pow(10, 1)) {
            prime = 11;
            generator = 2;
        } else if (cardinality <= Math.pow(10, 2)) {
            prime = 101;
            generator = 7;
        } else if (cardinality <= Math.pow(10, 3)) {
            prime = 1009;
            generator = 26;

        } else if (cardinality <= Math.pow(10, 4)) {
            prime = 10007;
            generator = 59;

        } else if (cardinality <= Math.pow(10, 5)) {
            prime = 100003;
            generator = 242;

        } else if (cardinality <= Math.pow(10, 6)) {
            prime = 1000003;
            generator = 568;
        } else if (cardinality <= Math.pow(10, 7)) {
            prime = 10000019;
            generator = 1792;
        } else if (cardinality <= Math.pow(10, 8)) {
            prime = 100000007;
            generator = 5649;
        } else if (cardinality <= Math.pow(10, 9)) {
            prime = 2147483647;
            generator = 16807;
        } else {
            throw new IllegalArgumentException("Cannot generate rows more than 10^9");
        }
        schema.setGenerator(generator);
        schema.setPrime(prime);
    }
}
