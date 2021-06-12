# PANDORA

<span style="font-variant:small-caps;">Pandora</span> is a tool that automatically and continuously mines data from different existing tools and online platforms and enable to run and continuously update the results of MSR studies. 

In details, <span style="font-variant:small-caps;">Pandora</span> provides different benefits to: 
+ ___Continuous Dataset Creation___. <span style="font-variant:small-caps;">Pandora</span> enables to continuously mine data from repositories (e.g. [GitHub](https://github.com)), Issue trackers (e.g. [Jira](https://www.atlassian.com/software/jira)), and any online platform (e.g. [SonarCloud](https://sonarcloud.io)). Developers can add new plug-ins to develop new connectors to collect data from any other platform or standalone tool (e.g. [PyDriller](https://pydriller.readthedocs.io/en/latest/)). 
   

+ ___Continuous application of custom statistical and  machine learning models___. Researchers can upload their python scripts to analyze the data and schedule a training frequency (e.g. once a month).  


+ ___Simple and replicable data analysis approach___. Researchers do not need to know how to mine the data, but they can simply use them. 


+ ___Data Visualization___. Dashboard for visualizing the results of the study


+ ___Dataset export for offline usage___. Data scientist and software engineers can easily download the last version of the dataset and use it for their empirical studies.

## How it works

<span style="font-variant:small-caps;">Pandora</span> is composed by four main components:

+ ___Repository Mining___: aimed at Extracting information from repository, Transform and Load into the database (ETL). The process is based on ETL plugin that can be either API based, or executed on the locally cloned repositories. 


+ ___Data Analysis___: enables to integrate data-analysis plugins that will be executed in [Apache Spark](https://spark.apache.org), each using a specific methodology (Machine Learning/Statistical Analysis) to solve a specific task. 


+ ___Dashboard___: visualization tool based on [Apache Superset](https://superset.apache.org) , used for inspecting and visualize the data and the results of the analysis performed in the Data Analysis block.


+ ___Scheduler___: based on [Apache AirFlow](https://airflow.apache.org), aimed to interact with the other blocks in order to schedule the execution of (i) the repository mining, and (ii) the training/fitting of the models used in the Data Analysis block.  


+ __Download Platform__: to enable the download of the dataset collected.


## PANDORA Overall Design 

![alt text][logo]

[logo]: https://github.com/PANDORA/images/pandora_design.png "PANDORA design"

## How To Install 
### Software Requirements 




## Plug-ins development 

### How to create new ETL plug-ins 

### How to create new analysis plug-ins

### How to add dashboard components 

