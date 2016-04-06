# json-to-parquet
## From JSON to Parquet Using Thrift and Hadoop

I wanted to accomplish one thing: Take files that contain JSON objects, convert them into Thrift objects and store them in a Parquet file using a Hadoop job.

Read more in [this post](https://medium.com/@jonathanmv/from-json-to-parquet-using-thrift-and-hadoop-94baaec9629b)

Steps I followed:

1. Create a maven project
2. Get the JSON files ready
3. Create a Thrift schema
4. Generate Thrift classes from the schema
5. Create a converter that receives a JSON string and returns a Thrift object
6. Create a Mapper that receives a JSON string and emits a Thrift object
7. Create a job that reads the JSON files and saves the Parquet file
