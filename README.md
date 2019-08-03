# particle-kriging
Particle Kriging contains scripts for the Apache Airflow workflow management tool that is used to build a learning and prediction system that teaches an algorithm based on the geostatistical "Ordinary Kriging" method at periodic intervals and predicts particulate matter for locations without a measurement station. Additionally, a validation framework will also be set up to demonstrate the functional performance of the algorithm through cross-validation and to optimize the hyperparameters using grid-search.

All is packed into one Docker container and flexibly runnable.

## How to run

To run the kriging workflows `live_kriging` and `validate_kriging` first the airflow docker container must be built and updated, respectively.

```
sh docker-update.sh
```

Then, persistent volumes for results and the airflow postgres metadata tables must be created (to be done once).

```
docker volume create airflow-postgresql-volume
docker volume create airflow-results
```

After the images are created the airflow composition must be run.  
For the Local Executor run

```
docker-compose up
```

To use the Celery executor run

```
docker-compose -f docker-compose-CeleryExecutor.yml up
```

The running airflow instance can be browsed at `localhost:9090/admin`.

There the Workflow DAGs can be operated and triggered.

Results of the tasks can be found in docker container at `/usr/local/airflow/results` or in the docker volume `airflow-results`.

## Workflows

The following kriging workflows are implemented:

Live Kriging:

```
get_raw_data >> distance_matrix >> variogram_cloud >> empirical_variogram >> semivariogram
[grid, get_raw_data, distance_matrix, semivariogram] >> kriging >> result
```

Validate Kriging:

```
For each grid in grid search:
get_parameters >> get_raw_data >> cross_validate_split >> [train, test]

For each cross validation fold:
train >> distance_matrix >> variogram_cloud >> empirical_variogram >> semivariogram
test >> grid
[train, grid, distance_matrix, semivariogram] >> kriging
[kriging, test] >> statistics >> result
```

## Results

For each step of the kriging workflow you can find the corresponding data at result path `/usr/local/airflow/results`.

Also the statistics (RMSE against test data) and the final result of live kriging (predicted value grid) are printed in last task.
