cd /home/bigd
PYSPARK_PYTHON=./sparkvenv/bin/python /opt/spark/bin/spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./sparkvenv/bin/python --master yarn --deploy-mode client --archives sparkvenv.tar.gz#sparkvenv population_density.py
PYSPARK_PYTHON=./sparkvenv/bin/python /opt/spark/bin/spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./sparkvenv/bin/python --master yarn --deploy-mode client --archives sparkvenv.tar.gz#sparkvenv CommScore.py
PYSPARK_PYTHON=./sparkvenv/bin/python /opt/spark/bin/spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./sparkvenv/bin/python --master yarn --deploy-mode client --archives sparkvenv.tar.gz#sparkvenv map_gen.py
