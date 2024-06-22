[[Todo]]
# Dependencies

 - [[Customer Partition Assignor API]]
# Description
The queue manager needs to be able to determine what source topics are dependencies for others. By constructing a record of of dependencies, once we have access the API for custom partition assignors, we can distribute pipeline parts evenly across all app instances.

Consider the following example:

![[TaskIsloation.png]]
If each topic has 1 partition, than currently we can only run the application on a single instance. Once we have the ability to map isolated tasks and have access to the partition assignor, we can assign Task A to some instance and Task B to another.