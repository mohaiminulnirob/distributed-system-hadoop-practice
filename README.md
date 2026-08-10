# distributed-system-hadoop-practice# Hadoop Lab-Final Practice Kit

Three MapReduce programs, from basic to advanced, plus a command cheat sheet.
Run these on the WSL2 Hadoop install (Java 11, Hadoop 3.3.6).

## Files

| File | Concept it drills |
|---|---|
| `src/WordCount.java` | The basic Mapper → Shuffle/Sort → Reducer flow, Combiners |
| `src/AverageWordLengthByLetter.java` | Custom `Writable` types, why averages can't use a naive combiner |
| `src/TopNWords.java` | Forcing a single reducer for a global result, local pre-aggregation |
| `data/sample.txt` | Sample input text to run all three against |

---

## 1. Compile and package (do this for each program)

```bash
# from the hadoop-practice directory
mkdir -p classes

# compile against Hadoop's own jars
javac -classpath $(hadoop classpath) -d classes src/WordCount.java

# package into a runnable jar
cd classes && jar -cvf ../wordcount.jar . && cd ..
```

Repeat the same two commands for `AverageWordLengthByLetter.java` → `avgwordlen.jar`
and `TopNWords.java` → `topn.jar` (compile into the same `classes/` folder is fine,
since the class names don't collide).

## 2. Put the sample data into HDFS

```bash
hdfs dfs -mkdir -p /practice/input
hdfs dfs -put data/sample.txt /practice/input
```

## 3. Run WordCount

```bash
hadoop jar wordcount.jar WordCount /practice/input /practice/wc-output
hdfs dfs -cat /practice/wc-output/part-r-00000
```

## 4. Run the average-word-length program

```bash
hadoop jar avgwordlen.jar AverageWordLengthByLetter /practice/input /practice/avg-output
hdfs dfs -cat /practice/avg-output/part-r-00000
```

## 5. Run Top-N (chained on top of WordCount's output)

```bash
hadoop jar topn.jar TopNWords /practice/wc-output /practice/topn-output
hdfs dfs -cat /practice/topn-output/part-r-00000
```

**Important:** if you re-run any job with the same output path, Hadoop will error
with "output directory already exists." Either delete it first or use a new path:

```bash
hdfs dfs -rm -r /practice/wc-output
```

---

## Java refresher — the patterns you'll see in every MapReduce program

- **`extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>`** — generics declare the
  input/output types. You override `map()`.
- **`extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>`** — `VALUEIN` in `reduce()`
  always arrives as `Iterable<VALUEIN>` — one call per *key*, with all its values grouped.
- **`context.write(key, value)`** — how both Mapper and Reducer emit output.
- **`Writable` interface** — anything sent between Map and Reduce (or written to disk)
  must implement `write(DataOutput)` and `readFields(DataInput)`. Built-ins:
  `Text`, `IntWritable`, `LongWritable`, `DoubleWritable`, `NullWritable`. Roll your own
  (like `SumCount` in the average example) when one value isn't enough.
- **Driver / `main()`** — configures a `Job`: which Mapper/Reducer/Combiner classes,
  what key/value types, and the input/output paths, then calls `waitForCompletion()`.

---

## HDFS command drill (do these from memory)

```bash
hdfs dfs -mkdir /dir
hdfs dfs -ls /
hdfs dfs -put localfile.txt /dir
hdfs dfs -get /dir/localfile.txt
hdfs dfs -cat /dir/localfile.txt
hdfs dfs -cp /src /dst
hdfs dfs -mv /src /dst
hdfs dfs -rm /dir/file.txt
hdfs dfs -rm -r /dir
hdfs dfs -du -h /
hdfs dfs -df -h /
hdfs dfsadmin -report          # cluster health summary
hdfs fsck /dir -files -blocks  # block-level details, good for fault-tolerance questions
```

## Theory checklist — be able to explain each in 1-2 sentences

- Why HDFS splits files into blocks, and the default block size (128 MB).
- NameNode = metadata only; DataNode = actual block storage.
- Why replication factor 3 is standard in production, and why it's 1 here.
- What the Secondary NameNode actually does (checkpointing, **not** failover).
- ResourceManager vs. NodeManager, and what a YARN "container" is.
- Map → Shuffle/Sort → Reduce, and why the same key always reaches the same reducer.
- Why a Combiner is safe for `sum`/`count`/`max` but **not** for `average` (see
  `AverageWordLengthByLetter.java` for the fix — carry sum and count, not the ratio).
- CAP-theorem-style trade-offs: how HDFS favors consistency + partition tolerance
  for metadata, while striving for availability of data via replication.

## Common exam pitfalls

- Forgetting `job.setMapOutputKeyClass` / `setMapOutputValueClass` when the Mapper's
  output types differ from the job's final output types (they often do, as in `TopNWords`).
- Using a Combiner on non-associative/non-commutative operations (like raw averages).
- Not handling the "output directory already exists" error before re-running a job.
- Confusing `hdfs dfs -rm` (moves to trash if enabled) with permanent deletion
  (`-skipTrash` flag).
