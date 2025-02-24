# Shared variables in Spark

Shared variables are variables that are used by multiple functions and methods simultaneously. They are used to enable efficient data sharing and consistency across nodes. <br>

2 types of shared variables in Spark<br>

1. Accumulators : Variables that are used to aggregate information across all tasks in a Spark job. They are used to maintain counters and sums in parallel computations. <br>

2. Broadcast : An optimization technique that caches a variable on each machine instead of shuffling it around. This reduces data shuffling across nodes. <br>

How shared variables are used<br>

Shared variables allow for efficient data sharing and consistency across nodes. <br>
Accumulators are used to accumulate values across multiple tasks in a Spark application. They are designed to store intermediate results that can be accumulated over time. <br>
Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. <br>

Other details <br>

Accumulators are write-only variables.<br>

Broadcast variables are immutable, that is, once initialized their value cannot be changed.<br>
