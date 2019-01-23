# jmeter-mongo-db-custom-sampler
Reactive streams based jmeter mongo db custom sampler

### Compile
To compile simply run:
```
mvn package
```

###Usage
This was tested with jmeter 5.0. To use copy the resulting jar (from the target directory) to 
jmeter's lib/ext directories. 

Also copy (should work with other versions as well):
* mongo-java-driver-3.9.1.jar
* mongodb-driver-reactivestreams-1.10.0.jar
* reactive-streams-1.0.2.jar
 
The class will show up at the Java Sampler dropdown menu.
All of the params need to be filled in including the key and value.
Use Jmeter's value generator to randomize these values or read 
them from a file.