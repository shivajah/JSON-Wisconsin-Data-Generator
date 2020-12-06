# JSON Wisconsin Data Generator: To JSON and Beyond
[JSON Wisconsin Data Generator](https://github.com/shivajah/JSON-Wisconsin-Data-Generator) is a [Wisconsin](http://jimgray.azurewebsites.net/benchmarkhandbook/chapter4.pdf)-inspired data generator tool implemented in Java. Wisconsin Benchmark was designed at the University of Wisconsin by DeWitt et al to test the performance of the major components of a relational database system while the semantics and statistics of its relations would be easy to understand.  This Benchmark was initially a single-user micro-benchmark for measuring individual query performance. A multi-user version of this benchmark which was developed a few years later did not attract as much as attraction as past, due to existence of other well-established competitors such as DebitCredit Benchmark, a multi-user OLTP benchmark led by Jim Gray.
While the Wisconsin Benchmark does not have as large an audience today as in the past, we believe that its simple but powerful design easy to understand and highly controllable relations and attributes are unique and can be beneficial in evaluating database systems from many angles. As such, we have built a Wisconsin-inspired JSON data generator in java with more add-ons and advances features to support more modern data features, including: Variable length records based on size distributions, skewed attributes, and other features to support semi-structured data behavior as well.
This data generator has been used in several studies and publications for benchmarking [AsterixDB](https://asterixdb.incubator.apache.org).

## Wisconsin Benchmark:
Figure below shows the schema for the Wisconsin Benchmark which is the default schema for our JSON Wisconsin Data Generator. 
<p align="center">
    <img  src="https://github.com/shivajah/JSON-Wisconsin-Data-Generator/blob/main/Wisconsin%20Benchmark-%20Original%20Schema.png">
</p>

## JSON Wisconsin Data Generator:

### Inputs:
The following JSON shows how this schema looks like in JSON to be used as an input to the program. A JSON array is used to show the schema for a relation while the JSON objects included in this array make the relation attributes. User can add more attributes by adding a new JSON object in the JSON array or drop them by simply removing them from the array.

```JSON
[
  {
  "name":"unique1",
  "type":"integer",
  "order":"random"
},
  {
    "name":"unique2",
    "type":"integer",
    "order":"sequential"
  },
  {
    "name":"two",
    "type":"integer",
    "order":"random",
    "range":2
  },
  {
    "name":"four",
    "type":"integer",
    "order":"random",
    "range":4
  },
  {
    "name":"ten",
    "type":"integer",
    "order":"random",
    "range":10
  },
  {
    "name":"twenty",
    "type":"integer",
    "order":"random",
    "range":20
  },
  {
    "name":"onePercent",
    "type":"integer",
    "order":"random",
    "range":100
  },
  {
    "name":"tenPercent",
    "type":"integer",
    "order":"random",
    "range":10
  },
  {
    "name":"twentyPercent",
    "type":"integer",
    "order":"random",
    "range":5
  },
  {
    "name":"fiftyPercent",
    "type":"integer",
    "order":"random",
    "range":2
  },
  {
    "name":"unique3",
    "type":"integer",
    "order":"random"
  },
  {
    "name":"evenOnePercent",
    "type":"integer",
    "order":"random",
    "even":true,
    "range":100
  },
  {
    "name":"oddOnePercent",
    "type":"integer",
    "order":"random",
    "odd":true,
    "range":100
  },
  {
    "name":"stringu1",
    "type":"string",
    "length":"100",
    "order":"sequential"
  },
  {
    "name":"stringu2",
    "type":"string",
    "length":"100",
    "order":"random"
  },
  {
    "name":"string4",
    "type":"string",
    "length":"400",
    "order":"random"
  }
]

```

### Outputs:

The output of the program is a set of JSON objects where each object represents a record in the relation. Figure 3 shows an example of 5 output records.

```JSON
{"unique1":2, "unique2":1, "two":0, "four":2, "ten":2, "twenty":2, "onePercent":2, "tenPercent":2, "twentyPercent":2, "fiftyPercent":0, "unique3":2, "evenOnePercent":4, "oddOnePercent":5, "stringu1":"BAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "stringu2":"CAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "string4":"CAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
{"unique1":4, "unique2":2, "two":0, "four":0, "ten":4, "twenty":4, "onePercent":4, "tenPercent":4, "twentyPercent":4, "fiftyPercent":0, "unique3":4, "evenOnePercent":8, "oddOnePercent":9, "stringu1":"CAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "stringu2":"EAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "string4":"EAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
{"unique1":1, "unique2":3, "two":1, "four":1, "ten":1, "twenty":1, "onePercent":1, "tenPercent":1, "twentyPercent":1, "fiftyPercent":1, "unique3":1, "evenOnePercent":2, "oddOnePercent":3, "stringu1":"DAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "stringu2":"BAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "string4":"BAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
{"unique1":5, "unique2":4, "two":1, "four":1, "ten":5, "twenty":5, "onePercent":5, "tenPercent":5, "twentyPercent":0, "fiftyPercent":1, "unique3":5, "evenOnePercent":10, "oddOnePercent":11, "stringu1":"EAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "stringu2":"FAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "string4":"FAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
{"unique1":3, "unique2":5, "two":1, "four":3, "ten":3, "twenty":3, "onePercent":3, "tenPercent":3, "twentyPercent":3, "fiftyPercent":1, "unique3":3, "evenOnePercent":6, "oddOnePercent":7, "stringu1":"FAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "stringu2":"DAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "string4":"DAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}

```

### Usage:
1.  Checkout the [JSON Wisconsin Data Generator](https://github.com/shivajah/JSON-Wisconsin-Data-Generator) in a directory using git. In this document we assume that the path of the mentioned directory is $HOME/JsonWiscDataGen and we will refer to it as JSONWISCDATAGEN_HOME. You can choose any other directory as the home directory for JSON Wisconsin Data Generator.
2.  Set the $HOME/JsonWiscDataGen as an environment variable on your machine using the following command:
> export JSONWISCDATAGEN_HOME=$HOME/JsonWiscDataGen
3.   Go to the home directory of JSON Wisconsin Data Generator
> cd $JSONWISCDATAGEN_HOME
4.  Build the data generator using maven as follow:
> mvn clean package 
5.  If the build is successful, a target folder will be created which contains the required jar file and dependencies. The workload file providing the schema and other relation information for Json Wisconsin Data Generator should be created as a json file in '$JSONWISCDATAGEN_HOME/Workloads' directory. There are some example workloads including the default workload in this folder. You can start creating your own workload file using these examples and information provided here.
6. You can start generating the dataset by running the following command:

> java -jar $JSONWISCDATAGEN_HOME/target/wisconsin-datagen.jar writer=file workload=default.json  cardinality=99999999

7. Default configurations are provided in $JSONWISCDATAGEN_HOMEB/src/main/resources/wisconsin_datagen.properties which can be overwritten similar to the above example (step 6).
-   filesize: In MB, which would be another terminator for the program. Program will stop generating records if the asked cardinality of filesize is reached. Whichever that happen first, would be the terminator.
-   writer: asterixdb or file. "file" writes the output to a file in target folder with the name provided in "fileoutput". "asterixdb" writer loads the records directly to AsterixDB using AsterixDBLoadPort. For using the asterixdb writer, data generator should run on one of the NC nodes for it in order to work properly.
