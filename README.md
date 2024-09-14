# hadoopmetrics
simple service polling all base directories of a path, getting the size of the folder and save this pair of information in a hashmap.
This hashmap is used to create prometheus metrics for each folder.

# urls
* Get metrics: http://localhost:8080/actuator/prometheus
* Manual start of metrics generation (will only update every hour) http://localhost:8080/metricsstart

  
