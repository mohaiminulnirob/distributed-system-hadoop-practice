# Distributed Systems and Hadoop Practice

A collection of Java and Hadoop MapReduce exercises for practicing distributed data processing, HDFS, and mapper-reducer workflows.

## Topics and Practice

- Hadoop Distributed File System (HDFS) commands and file management
- Java MapReduce programs with custom mappers and reducers
- Word count and distinct-word counting
- Word, character, sentence, line, and whitespace analysis
- Counting words by length, starting letter, or ending character
- Finding longest words, largest lines, and most common word lengths
- Average word length, salary calculations, and top-N word analysis
- Input splitting and running jobs with multiple mappers and reducers

## Repository Structure

- `src/`: Basic Java data-processing exercises
- `Practice/`: Individual MapReduce practice problems
- `Question1/`, `Question3/`: Assignment solutions
- `LabFinal/`, `WordCountLabFinal/`: Final lab MapReduce programs and JAR files
- `data/`: Sample input data
- `instructions.txt`: Hadoop setup and execution commands

## Typical Workflow

1. Start SSH, HDFS, and YARN services.
2. Create an input directory in HDFS and upload the input file.
3. Compile the Java MapReduce program using the Hadoop classpath.
4. Package the compiled classes into a JAR file.
5. Run the job with `hadoop jar`.
6. Inspect the generated output in HDFS.

The exercises are intended for hands-on practice with Hadoop command-line tools,
HDFS storage, distributed processing, and MapReduce result analysis.
