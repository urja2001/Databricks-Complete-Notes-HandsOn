# Spark Architecture ✨

<div align="center">
<img align="center" alt="spark" src="https://github.com/urja2001/Databricks-Complete-Notes-HandsOn/blob/757cc322b1305084c643da90fda25d16eae9c97a/Chapter%200%20-%20Architecture%20of%20Spark/pics/SparkArchitecture_1.jpg" width="500" height="400" style="border-radius:50%">
</div>

1. The driver program in the Apache Spark architecture is called the main program of an application and creates <b>SparkContext</b>. A SparkContext consists of all the basic functionalities. <br>

2. <b>Spark Driver</b> contains various other components, such as DAG Scheduler, Task Scheduler and Block Manager, which are responsible for translating the user-written code into jobs that are actually executed on the cluster.<br>

3. <b>Spark Driver and SparkContext collectively watch over the job execution within the cluster.</b> <br>

4. Spark Driver works with the <b>cluster Manager</b> to manage various other jobs.
   
6. The <b>Cluster Manager</b> does the resource-allocating work. Then, the job is split into multiple smaller tasks, which are further distributed to worker nodes.<br>

7. Whenever an RDD is created in the SparkContext, it can be distributed across many worker nodes and cached there.<br>
8. Worker nodes execute the tasks assigned by the Cluster Manager and return it back to the Spark Context.<br>

9. An executor is responsible for the execution of these tasks. The lifetime of executors is the same as that of the Spark Application. <br>
10. If we want to increase the performance of the system, we can increase the number of workers so that the jobs can be divided into more logical portions.<br>

<div align="center">
<img align="center" alt="spark" src="https://github.com/urja2001/Databricks-Complete-Notes-HandsOn/blob/9a9aa6d73642e738a968dedd57c2612d4fc8822a/Chapter%200%20-%20Architecture%20of%20Spark/pics/SparkArchitecture_2.jpg" width="600" height="300" style="border-radius:50%">
</div>
