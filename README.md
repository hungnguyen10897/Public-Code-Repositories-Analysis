# PANDORA

<span style="font-variant:small-caps;">Pandora</span> is a tool that automatically and continuously mines data from different existing tools and online platforms and enable to run and continuously update the results of MSR studies.

In details, <span style="font-variant:small-caps;">Pandora</span> provides different benefits to: 
+ ___Continuous Dataset Mining___. <span style="font-variant:small-caps;">Pandora</span> is designed to continuously mine data from repositories (e.g. [GitHub](https://github.com)), Issue trackers (e.g. [Jira](https://www.atlassian.com/software/jira)), and any online platform (e.g. [SonarCloud](https://sonarcloud.io)).
   

+ ___Continuous application of custom Statistical and  Machine Learning models___. Researchers can upload their python scripts to analyze the data and schedule a training frequency (e.g. once a month).  


+ ___Simple and replicable data analysis approach___. Researchers do not need to know how the data is incrementally updated, they can simply use them. 


+ ___Data Visualization___. Dashboards for visualizing the results of the study


+ ___Dataset export for offline usage___. Data scientist and researchers can easily download the lastest versions of the datasets for their empirical studies.


+ ___Mutual Platform for further integrations___. Developers can build new plug-ins for other datasources, platforms or standalone tools (e.g. [PyDriller](https://pydriller.readthedocs.io/en/latest/)) by integrating their ETL, analysis/processing pipelines, scheduling them with Airflow, sharing the same backend database and visualization tool.

## Components

<span style="font-variant:small-caps;">Pandora</span> is composed by four main components:

+ ___Data Extraction___: aimed at Extracting information from repository, Transform and Load into the database (ETL). The process is based on ETL plugin that can be either API based, or executed on the locally cloned repositories. 


+ ___Data Processing___: enables to integrate data-analysis plugins that will be executed in [Apache Spark](https://spark.apache.org), each using a specific methodology (Machine Learning/Statistical Analysis) to solve a specific task. 


+ ___Dashboard___: visualization tool based on [Apache Superset](https://superset.apache.org) , used for inspecting and visualize the data and the results of the analysis performed in the Data Analysis block.


+ ___Scheduler___: based on [Apache AirFlow](https://airflow.apache.org), aimed to interact with the other blocks in order to schedule the execution of (i) the repository mining, and (ii) the training/fitting of the models used in the Data Analysis block.  


+ __Registration/Download Website__: enable registering project repositories for analysis or downloading the collected datasets (the link can be found at the [main dashboard](http://sqa.rd.tuni.fi/r/1) info section)


## Architecture

![alt text][logo]

[logo]: https://raw.githubusercontent.com/clowee/PANDORA/master/images/pandora_design.png "PANDORA design"

## Project Structure

```
.
├── README.md
├── config.cfg                  # General Configurations for the project
├── data_processing             # Spark Data Processing
├── db                          # Backend database
├── extractors                  # Extractor modules
├── images
├── installation_guide.md   
├── requirements.txt            # Python env packages
├── scheduler                   # Apache Airflow tasks and DAGs
├── ui                          # Apache Superset exported templates
└── utils.py                    # Utilities consumed by all other parts
```

## Dashboards
+ [Project Build Stability](http://sqa.rd.tuni.fi/r/1)
+ [Machine Learning Models](http://sqa.rd.tuni.fi/r/2)


## Importing Static Datasets for Visualization/Analysis

It is possible to import static datasets into the Visualization tool (Apache Superset). This gives users the opportunity to interactively visualize, analyze and find connections between their data and data readily available on the platform.

1. If you have a database, you can create a connection to your database by crafting a SQLAlchemy URI, then fill in necessary information at **Data** -> **Databases** -> **Add a new record** or follow [this page](http://sqa.rd.tuni.fi/databaseview/add).

2. Specify the tables you want to import. Go to **Data** -> **Datasets** or [this page](http://sqa.rd.tuni.fi/tablemodelview/add). You can simply specify a table using SQL, e.g ```SELECT * FROM <TABLE_NAME>```, or use a more complex SQL command to customize your view/table.

3. If you have a CSV file. First, you need to create a backend database and wire it up with Superset (follow the Step 1). In the settings of the database, tick on **Allow Csv Upload** property, and specify **schemas_allowed_for_csv_upload** in the **Extra** section, e.g put "public". Now go to **Data** -> **Upload a CSV** or [this page](http://sqa.rd.tuni.fi/csvtodatabaseview/form), fill in the settings to upload the CSV into a database.
 
