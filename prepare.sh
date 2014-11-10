#!/bin/bash
javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar -d question-1/wordcount_classes question-1/WordCount.java
jar -cvf question-1/wordcount.jar -C question-1/wordcount_classes/ .

javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar -d question-1/topcount_classes question-1/TopCount.java
jar -cvf question-1/topcount.jar -C question-1/topcount_classes/ .

# java-json.jar has to be added to the lib/ folder of hadoop for question 2. It can be done by:
#
# $ cd /home/hadoop/hadoop-1.2.1/lib
# $ wget http://www.java2s.com/Code/JarDownload/java/java-json.jar.zip
# $ unzip java-json.jar.zip
#
javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-1.2.1/lib/java-json.jar -d question-2/combinebooks_classes question-2/CombineBooks.java
jar -cvf question-2/combinebooks.jar -C question-2/combinebooks_classes/ .

javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-1.2.1/lib/java-json.jar -d question-2/queryauthor_classes question-2/QueryAuthor.java
jar -cvf question-2/queryauthor.jar -C question-2/queryauthor_classes .
