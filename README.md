# stedi-human-balance-analytics_spark_datalakes
STEDI Human Balance Analytics (SPARK, DATA LAKES, AWS Glue, AWS Athena, AWS S3)

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS

All th python scripts are present in "glueJob_python_scripts" folder

![Dexcription](./images/flowchart.jpeg)

**Landing Zone (Raw Data)**

Customer: 956
![Dexcription](./images/customer_landing.png)

Accelerometer: 81273

![Dexcription](./images/accelerometer_landing.png)

Step Trainer: 28680

![Dexcription](./images/step_trainer_landing.png)

The customer landing data has multiple rows where shareWithResearchAsOfDate is blank.

![Dexcription](./images/shareofresearch.png)

**Trusted Zone (Filtering,PII)**

Customer: 482

![Dexcription](./images/customer_trusted.png)

Accelerometer: 40981

![Dexcription](./images/accelerometer_trusted.png)

Step Trainer : 40981

![Dexcription](./images/step_trainer_trusted.png)

Customer Trusted (table that shows no blank shareWithResearchAsOfDate row): Empty

**SELECT* FROM customer_trusted WHERE sharewithresearchasofdate IS NULL** : Empty Table

![Dexcription](./images/pii.png)

**Curated Zone**

Customer: 482

![Dexcription](./images/customer_curated.png)

Accelerometer: 40981

![Dexcription](./images/step_trainer_curated.png)

