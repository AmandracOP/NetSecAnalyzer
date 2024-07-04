# I started by creating the project structure and as I am in my arch envoirnment
## I directly runned this command to do so
```
mkdir -p project-root/{data/{raw,processed},notebooks,scripts,modules,reports,configs,environment} && \
cd project-root && \
touch notebooks/{exploration.ipynb,anomaly_detection.ipynb,trends_analysis.ipynb} && \
touch scripts/{data_generation.py,data_processing.py,analysis.py,aggregation.py} && \
touch modules/{__init__.py,data_aggregator.py,utils.py} && \
touch reports/{findings.md,next_steps.md,scalability.md} && \
touch configs/{spark_config.json,other_config.json} && \
touch environment/{requirements.txt,environment.yml} && \
touch README.md
```
