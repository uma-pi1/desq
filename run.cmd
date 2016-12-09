@ECHO OFF
setlocal enabledelayedexpansion

mvn package -Dmaven.test.skip=true
java -Xms4096m -Xmx4096m -Xss4m -classpath "target/desq-0.0.1-SNAPSHOT-jar-with-dependencies.jar" %1
