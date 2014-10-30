#!/bin/bash
javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar -d question-1/top_count_classes question-1/TopCount.java
jar -cvf question-1/topcount.jar -C question-1/top_count_classes/ .
javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar -d question-2/combine_books_classes question-2/CombineBooks.java
jar -cvf question-2/combinebooks.jar -C question-2/combine_books_classes/ .
javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar -d question-2/query_author_classes question-2/QueryAuthor.java
jar -cvf question-2/queryauthor.jar -C question-2/query_author_classes .
