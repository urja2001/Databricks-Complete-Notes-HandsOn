# Resilient Distributed Dataset

-- Resilient Distributed Datasets (RDD) is a fundamental data structure of Spark.<br>
-- It is an immutable distributed collection of objects. <br>
-- Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster. <br>
-- They are of 2 types  1. Transformation      2. Action
--

--
Transformations 
--
 -- Define the logical execution plan for data. <br>
 -- They are lazy, meaning they are only applied when an action is called. <br>
 -- Modify data structures, generating a new lineage <br>

--
Actions 
--
 -- Trigger the execution of the logical plan. <br>
 -- Produce a result or side effect. <br>
 -- Return values or save data to external storage. <br>

--
How transformations and actions work
--
 -- Transformations are the building blocks for constructing the logical flow of data. <br> 
 -- Actions tell the Spark engine to refer to the logical plan and execute the entire sequence of transformations. <br> 
 -- This lazy evaluation mechanism allows for optimal processing efficiency. <br> 

<div align="center">
<img align="center" alt="rdd" src="https://miro.medium.com/v2/resize:fit:720/format:webp/1*xGrIK4GU1PRZ49AMPTc-0w.png" width="500" height="400" style="border-radius:50%">
</div>
