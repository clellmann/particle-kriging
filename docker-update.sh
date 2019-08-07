docker rmi --force clellmann/kriging-airflow:1.10.4
docker rmi --force clellmann/kriging-airflow

docker build --rm -t clellmann/kriging-airflow .
docker tag clellmann/kriging-airflow:latest clellmann/kriging-airflow:1.10.4