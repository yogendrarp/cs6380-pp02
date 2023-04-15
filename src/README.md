# CS CS6380 Distributed Computing
___

## Project 02- Implementation of SyncGHS Algorithm
### Author Yogendra Prabhu

### Prerequistes
* JDK 1.8
* gnome-terminal

### Compile
To compile the code, place the files in the folder and run

`javac *.java`

In dc machines the path java by default points to java1.7, which doesnt support java lambdas. There for fullpath
must be provided. The path in dcmachines is 

`/usr/local/jdk1.8.0_341/bin/java`

### Local testing
The code supports localtesting, where in local system would assume as one of the nodes, running on the
port specific to the dc machine, Additionally bring up Synchronizer.

`java Synchronizer`

`java Node test dc02.utdallas.edu`

`java`- the binary

`Node` - Name of the class file where main method is present

`test` - specifies environment

`dc02.utdallas.edu`- assumes its port from the configuration file


### Production
To run the code on dc machines, use the launcher.sh file. Here no need to pass the hostname arg.
`java Node prod`