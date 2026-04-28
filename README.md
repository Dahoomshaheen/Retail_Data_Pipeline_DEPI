

تفصيل كامل لكل خطوة


---

الخطوة 1: تحميل الريبو

افتح Terminal وكتب:

git clone https://github.com/Dahoomshaheen/Retail_Data_Pipeline_DEPI.git
cd Retail_Data_Pipeline_DEPI


---

الخطوة 2: عمل ملف .env

افتح فولدر docker/ وهتلاقي ملف .env.example

اعمل ملف جديد في المجلد الرئيسي اسمه .env وحط فيه:

AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.8.1

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_db_secure_pass_2026
POSTGRES_DB=airflow

_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=Admin_Secure_Password_123

MSSQL_SA_PASSWORD=SuperStrong_SQL_Pass_2026!
ACCEPT_EULA=Y

_PIP_ADDITIONAL_REQUIREMENTS=pandas pyarrow pyodbc fastparquet pymssql


---

الخطوة 3: تشغيل Docker

تأكد إن Docker Desktop شغال على جهازك، بعدين:

cd docker
docker-compose up -d

استنى 2-3 دقايق وبعدين تأكد:

docker ps

المفروض تشوف:

postgres      → running
sqlserver     → running
airflow-webserver → running
airflow-scheduler → running


---

الخطوة 4: تحميل الداتا

روح Kaggle وحمل الداتا من:

(https://www.kaggle.com/competitions/walmart-recruiting-store-sales-forecasting/data)

حمل الملفات:

train.csv

stores.csv

features.csv


وحطهم في:

Retail_Data_Pipeline_DEPI/
└── data/
    └── raw/
        ├── train.csv
        ├── stores.csv
        └── features.csv


---

الخطوة 5: تشغيل سكريبت الـ SQL

docker exec -it docker-airflow-scheduler-1 python /opt/airflow/airflow_dags/ingest_stores.py

المفروض تشوف:

✅ تم رفع 45 صف على SQL Server


---

الخطوة 6: تشغيل سكريبت الـ Parquet

docker exec -it docker-airflow-scheduler-1 python /opt/airflow/airflow_dags/convert_to_parquet.py

المفروض تشوف:

✅ تم تحويل train.csv → train.parquet
✅ تم تحويل features.csv → features.parquet


---

الخطوة 7: كل واحد يشتغل على دوره

الدور 3 عمر- تنظيف البيانات:

افتح فولدر etl_python/

اشتغل على train.parquet و features.parquet من data/parquet/

عالج الـ Nulls في العروض

تعامل مع المبيعات السالبة


الدور 4 نور- Star Schema:

استلم الداتا النظيفة من الدور 3

ابني الـ Star Schema على SQL Server


الدور 5 عبدالرحمن - Power BI:

افتح فولدر visualization_powerbi/

اتصل بـ SQL Server على localhost:1433

ابني الـ Dashboards



---

### الخطوة 6 - أحمد  Airflow Orchestration (الدور 6)

الدور 6 بيعمل الآتي:

1. يسحب آخر تحديث من GitHub:

git pull origin main


---

2. يقرأ كل الملفات الموجودة في etl_python/ أوتوماتيك

كل ملف لازم يكون فيه function بنفس اسم الملف:

clean_data.py → clean_data()

build_star_schema.py → build_star_schema()



---

3. يبني pipeline_dag.py داخل:

airflow_dags/pipeline_dag.py

ويربط السكريبتات بالترتيب:

ingest_stores → convert_to_parquet → clean_data → build_star_schema

كل خطوة تتحول إلى Task باستخدام Airflow PythonOperator.


---

4. تشغيل Airflow (يدوي):

airflow db init
airflow scheduler
airflow webserver

ثم افتح:

http://localhost:8080


---

5. رفع الـ DAG على GitHub:

git add airflow_dags/pipeline_dag.py
git commit -m "feat: update pipeline DAG"
git push origin main


