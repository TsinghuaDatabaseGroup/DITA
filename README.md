DITA: Distributed In-Memory Trajectory Analytics
===========================================

DITA is a distributed in-memory trajectory analytics system based on Apache Spark 2.2.0.

Development
---------------

Since we use IntelliJ for development, you can consult [the official guide for setuping up the IDE](http://spark.apache.org/developer-tools.html). Besides, you should do the following things:

- Install the [ANTLR v4 plug-in for IntelliJ](https://plugins.jetbrains.com/plugin/7358-antlr-v4-grammar-plugin) which will be used in Section 1.
- Go to View > Tool Windows > Maven Projects and add `hadoop-2.6`, `hive-provided`, `hive-thriftserver`, `yarn` in `Profiles` (there are some default profiles as well, don't change them). Then **Reimport All Maven Projects** (the first button on upper-right corner), **Generate Sources and Update Folders For All Projects** (the second button on upper-right corner).
- Rebuild the whole project, which would fail but is essential for following steps.
- Marking Generated Sources:
    - Go to File > Project Structure > Project Settings > Modules. Find `spark-streaming-flume-sink`, and mark `target/scala-2.11/src_managed/main/compiled_avro` as source. (Click on the Sources on the top to mark)
    - Go to File > Project Structure > Project Settings > Modules. Find `spark-hive-thriftserver`, and mark `src/gen/java` as source. (Click on the Sources on the top to mark)
    - Go to File > Project Structure > Project Settings > Modules. Find `spark-hive-thriftserver`, and mark `src/gen/java` as source. (Click on the Sources on the top to mark)
- Rebuild the whole project again, which should work well now. If there still exist some compilation errors for not finding some classes, you may return to last step and marking corresponding sources if not included.

Examples
---------------

- [DITASQLExample](spark/examples/src/main/scala/org/apache/spark/examples/sql/dita/DITASQLExample.scala) for how to use SQL syntax for trajectory analytics
- [DITADataFrameExample](spark/examples/src/main/scala/org/apache/spark/examples/sql/dita/DITADataFrameExample.scala) for how to use DataFrame for trajectory analytics
- [DITAIndexExample](spark/examples/src/main/scala/org/apache/spark/examples/sql/dita/DITAIndexExample.scala) for how to create/drop/show indexes

Usage
---------------
The `master` branch is the version integrated with Spark SQL, and the `standalone` branch is a stand-alone version just with DITA code.

Contributors
------------
- Zeyuan Shang: zeyuanxy [at] gmail [dot] com
- Guoliang Li: liguoliang [at] tsinghua [dot] edu [dot] cn
- Zhifeng Bao: zhifeng.bao [at] rmit [dot] edu [dot] au
