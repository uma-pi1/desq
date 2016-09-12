@ECHO OFF
setlocal enabledelayedexpansion

rem Build class path
rem mvn dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
set /p classpath=<target\classpath.txt
set classpath=%classpath:;=,%,target\desq-0.0.1-SNAPSHOT.jar

rem For some reason, the driver does not seem to load the jars; so load them manually
rem we only load what we really need
IF EXIST target\spark-shell-require.scala del /F target\spark-shell-require.scala
for %%j in ("%classpath:,=" "%") do (
   set f=%%j
   set f=!f:"=! 

   set ff=!f:desq=!
   if not !f!==!ff! (
      echo :require !f! >> target\spark-shell-require.scala
   )

   set ff=!f:fastutil=!
   if not !f!==!ff! (
      echo :require !f! >> target\spark-shell-require.scala
   )
)

rem now run the shell
%SPARK_HOME%\bin\spark-shell --master local --jars %classpath% -i target\spark-shell-require.scala

