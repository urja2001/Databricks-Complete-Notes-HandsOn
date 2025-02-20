# Resilient Distributed Dataset

RDDs Stands for:<br>
Resilient: Fault tolerant and is capable of rebuilding data on failure.<br>
Distributed: Distributed data among the multiple nodes in a cluster.<br>
Dataset: Collection of partitioned data with values.<br>
--

-- It is a fundamental data structure of Spark i.e. They building blocks of any Spark application. <br>
-- It is an immutable distributed collection of objects. <br>
-- RDD lineage provides the foundation for Spark's fault tolerance by recording the sequence of transformations applied to data. <br>
-- Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster. <br>
--

There are two ways to create RDDs − <br>
    1. Parallelizing an existing collection in driver program.<br> 
    2. By referencing a dataset in an external storage system, such as a shared file system, HDFS, HBase, etc.
--

 RDDs can perform two types of operations:<br>
    1. Transformations: They are the operations that are applied to create a new RDD.<br>  
    2. Actions: They are applied on an RDD to instruct Apache Spark to apply computation and pass the result back to the driver.    
--



Transformations 
--
 -- Define the logical execution plan for data. <br>
 -- They are lazy, meaning they are only applied when an action is called. <br>
 -- Modify data structures, generating a new lineage <br>
 -- They are of 2 types  1. Narrow      2. Wide


Actions 
--
 -- Trigger the execution of the logical plan. <br>
 -- Produce a result or side effect. <br>
 -- Return values or save data to external storage. <br>


How transformations and actions work
--
 -- Transformations are the building blocks for constructing the logical flow of data. <br> 
 -- Actions tell the Spark engine to refer to the logical plan and execute the entire sequence of transformations. <br> 
 -- This lazy evaluation mechanism allows for optimal processing efficiency. <br> 

<div align="center">
<img align="left" alt="rdd" src="https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2018/09/Picture1-5-768x266.png" width="400" height="400" style="border-radius:50%">
<img align="right" alt="rdd" src="https://miro.medium.com/v2/resize:fit:720/format:webp/1*xGrIK4GU1PRZ49AMPTc-0w.png" width="400" height="400" style="border-radius:50%">
</div>
