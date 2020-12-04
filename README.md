JSON Wisconsin Data Generator is a Wisconsin-inspired data generator tool implemented in Java. It is inspired by the specification provided in "The wisconsin benchmark: Past, present, and future" written by David DeWitt in 1993. Wisconsin Benchmark was designed at the University of Wisconsin by DeWitt et al to test the performance of the major components of a relational database system while the semantics and statistics of its relations would be easy to understand.  This Benchmark was initially a single-user micro-benchmark for measuring individual query performance. A multi-user version of this benchmark which was developed a few years later did not attract as much as attraction as past, due to existence of other well-established competitors such as DebitCredit Benchmark, a multi-user OLTP benchmark led by Jim Gray.
While the Wisconsin Benchmark does not have as large an audience today as in the past, we believe that its simple but powerful design easy to understand and highly controllable relations and attributes are unique and can be beneficial in evaluating database systems from many angles. As such, we have built a Wisconsin-inspired JSON data generator in java with more add-ons and advances features to support more modern data features, including: Variable length records based on size distributions, skewed attributes, and other features to support semi-structured data behavior as well.
Figure 1 shows the schema for the Wisconsin Benchmark which is the default schema for our JSON Wisconsin Data Generator. 

![Original Wisconsin Benchmark Schema]
(https://github.com/shivajah/JSON-Wisconsin-Data-Generator/blob/main/Wisconsin%20Benchmark-%20Original%20Schema.png)

Inputs:

Figure 2 shows how this schema looks like in JSON to be used as an input to the program. A JSON array is used to show the schema for a relation while the JSON objects included in this array make the relation attributes. User can add more attributes by adding a new JSON object in the JSON array or drop them by simply removing them from the array.

Outputs:

The output of the program is a set of JSON objects where each object represents a record in the relation. Figure 3 shows an example of 5 output records.

Steps:
> export DATAGEN_HOME=$HOME/datagen

> cd $DATAGEN_HOME

> mvn clean package 

> $DATAGEN_HOME/scripts/run_datagen.sh workload=wisconsin_1GB_std_zero_fixedLength_noBigObject.json writer=couchbase(default = file)
