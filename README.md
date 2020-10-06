### Dry run the stream pipeline


To test the dry-run pipelines, please ensure:

1. Python 3.6.9 or 3.7 is installed or activate your `venv` or `conda`  environment with correct python version
2. Install the dependency by using 
    ```bash
    pip install -r requirements.txt
    ```
3. Make sure `java` is installed
    ```bash
    java --version
    ```
4. Execute the pipeline by using command:
    ```bash
 	bash dry_runner.sh -i <path_to_csv>
     ```
	*The csv file should NOT contain the header line*.

	The aggregated metrics data will be output to `./tmp/data/event_agg.csv `

Correctly formatted csv file samples are (no header):

```
A,2019-01-02 23:34:32+00:00,16304593690.0,460800.0
A,2019-01-01 17:52:54+00:00,16130641974.0,11764800.0
A,2019-01-05 15:36:55+00:00,16649306299.0,468000.0
A,2019-01-03 16:18:37+00:00,16445561883.0,15264000.0
```