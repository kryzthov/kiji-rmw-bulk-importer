This project contains an example of BulkImporter that reads from a KijiTable
using a KijiTableReader with explicit reader schemas.

You may compile and run the BulkImporter with:

$ mvn clean install
$ java \
    -classpath "$(find $PWD/target/RMWBulkImporter-0.0.1-SNAPSHOT-release/RMWBulkImporter-0.0.1-SNAPSHOT/ -name *.jar -printf %p:):$(hadoop classpath):$(hbase classpath)" \
    org.kiji.mapreduce.RMWBulkImporterRunner
