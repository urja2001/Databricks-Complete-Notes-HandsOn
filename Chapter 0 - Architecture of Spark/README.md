# Spark Architecture

<div align="center">
<img align="center" alt="spark" src="" width="500" height="400" style="border-radius:50%">
</div>

The driver program in the Apache Spark architecture is called the main program of an application and creates SparkContext. A SparkContext consists of all the basic functionalities. 
Spark Driver contains various other components, such as DAG Scheduler, Task Scheduler and Block Manager, which are responsible for translating the user-written code into jobs that are actually executed on the cluster.

Spark Driver and SparkContext collectively watch over the job execution within the cluster. Spark Driver works with the Cluster Manager to manage various other jobs. The cluster Manager does the resource-allocating work. Then, the job is split into multiple smaller tasks, which are further distributed to worker nodes.

Whenever an RDD is created in the SparkContext, it can be distributed across many worker nodes and cached there.

Worker nodes execute the tasks assigned by the Cluster Manager and return it back to the Spark Context.

An executor is responsible for the execution of these tasks. The lifetime of executors is the same as that of the Spark Application. If we want to increase the performance of the system, we can increase the number of workers so that the jobs can be divided into more logical portions.

<div align="center">
<img align="center" alt="spark" src="https://github.com/urja2001/Databricks-Complete-Notes-HandsOn/blob/9a9aa6d73642e738a968dedd57c2612d4fc8822a/Chapter%200%20-%20Architecture%20of%20Spark/pics/SparkArchitecture_2.jpg" width="600" height="300" style="border-radius:50%">
</div>
