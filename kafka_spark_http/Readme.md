### This project helps you run a local Spark cluster using Docker. It includes:

**Kafka & Zookeeper:** For streaming data.

**Spark Master:** Manages the cluster.

**Spark Worker:** Runs your code.

**Spark History Server:** Saves your job logs.

### Setup
Clone the project and create folders:
```
> git clone https://github.com/harshadt21/beam-spike.git
> cd kakfa_spark_http
> mkdir spark-jobs spark-conf
```
### Add your config:

In the spark-conf folder you can provide the desired config

###  Start the services:

```docker compose up -d```

###  Run a Spark Job
- Build your code: Create a single .jar file from your project beam-spike by running following command
```./gradlew clean shadowJar``` from the root
and put it in the spark-jobs folder under kafka_spark_http.


- Submit the job: Update the following command with the generated jar name and execute if from your terminal
It sends your job to the cluster.

```
docker exec -it spark-master spark-submit \
--class com.example.KafkaToRestApiSparkJob \
--master spark://spark-master:7077 \
--deploy-mode client /opt/spark-jobs/your-job-name.jar
```

- Check logs:

Exec into worker using
```docker exec -it spark-worker bash``` and then navigate to ```/opt/bitnami/spark/work``` to
read the app logs 

- Stop

To shut everything down, run:

```docker compose down```


### Http Server:

The dummy app has been created for exposing a rest api with a configurable rate limiter.
Also the app simulates the latency for each rest api call based on provided configuration

To start the app, run:
```python dummy_app.py```

### Auto Generate events on Kafka Topic:

Events are generated using a python script on the clickstream-events topic. 
To run the script execute `python click_events_script.py` 


### Objective:

Validate if rate limiting can be achieved by controlling the number of parallel tasks. Also the number of tasks should be decided using following formula

- L = Latency
- X = Number of tasks
- Q = QPS 

1 Record = L seconds (It takes L seconds to process 1 record for an api i.e Latency)

In one second, records process are
`1/L records/second`, hence each task will process 1/L records/second

If we have X tasks running, we will have to ensure
```X/L < QPS * (1 - L)```

For e.g

The rest api rate limit is 40 QPS, having 3 cores will avoid overwhelming the rest api:

Rest api with 40 QPS rate limit:
    ![Screenshot 2025-09-06 at 11.13.26 AM.png](Screenshot%202025-09-06%20at%2011.13.26%E2%80%AFAM.png)

Spark Cluster with 3 cores:
    ![Screenshot 2025-09-06 at 11.17.01 AM.png](Screenshot%202025-09-06%20at%2011.17.01%E2%80%AFAM.png)

Calculation to avoid exceeding rest api rate limit:
```
3 cores = 3 tasks
1 task can execute = 1000/100 = 1/0.1 = 10 calls
3 task can execute = 30 calls 

3/0.1 = 30 i.e < (40 * (1-0.1)) = (40 * 0.9) = 36
30 calls < 36 QPS
```






