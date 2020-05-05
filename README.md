# cs257_log_analyzer
* Log data set is represented by access_log_Aug95 file.
* Within the log_analyser folder, we have scripts written to execute commands.
  * Start script: Creates stream and topics
  * Producer script: Starts the Kafka producer
  * Spark script: Starts Spark application that performs log analysis
  * Consumer script: Starts the consumer to consume the processed logs

* The source code is present under 'src' folder and pom.xml can be used to build the project.
* Maven build command: 
```bash
mvn clean install DskipTests
```
