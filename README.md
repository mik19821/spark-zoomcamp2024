Instalacja spark:
    Wczesniej mozna zainstalowac Jupyter aby miec podreczny lab:
        jupyter notebook - startuje jupyter na porcie 8888. Domyslnie otwiera zakladke w przegladarce.

- **link do instrukcji instalacji:** 
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/linux.md
https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz
    1. tar xvzf openjdk-11.0.1_linux-x64_bin.tar.gz
    2. rm openjdk-11.0.1_linux-x64_bin.tar.gz
    3. export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
    4. export PATH="${JAVA_HOME}/bin:${PATH}"
    5. java --version



**komenda wydana z Jupyter**
`pip3 install pyspark`
>Collecting pyspark
  Downloading pyspark-3.5.1.tar.gz (317.0 MB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 317.0/317.0 MB 6.0 MB/s eta 0:00:00m eta 0:00:01[36m0:00:01
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
Collecting py4j==0.10.9.7 (from pyspark)
  Obtaining dependency information for py4j==0.10.9.7 from https://files.pythonhosted.org/packages/10/30/a58b32568f1623aaad7db22aa9eafc4c6c194b429ff35bdc55ca2726da47/py4j-0.10.9.7-py2.py3-none-any.whl.metadata
  Downloading py4j-0.10.9.7-py2.py3-none-any.whl.metadata (1.5 kB)
Downloading py4j-0.10.9.7-py2.py3-none-any.whl (200 kB)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 200.5/200.5 kB 11.0 MB/s eta 0:00:00
Building wheels for collected packages: pyspark
  Building wheel for pyspark (pyproject.toml) ... done
  Created wheel for pyspark: filename=pyspark-3.5.1-py2.py3-none-any.whl size=317488493 sha256=013650a8ddec65f295779dd9fa6267f97ec014d452b6510445634abe2e8483b6
  Stored in directory: /home/mik/.cache/pip/wheels/b1/91/5f/283b53010a8016a4ff1c4a1edd99bbe73afacb099645b5471b
Successfully built pyspark
Installing collected packages: py4j, pyspark
Successfully installed py4j-0.10.9.7 pyspark-3.5.1
[notice] A new release of pip is available: 23.2.1 -> 24.0
[notice] To update, run: pip install --upgrade pip

**Instalacja pyspark z shell**
```
Defaulting to user installation because normal site-packages is not writeable
Collecting pyspark
  Using cached pyspark-3.5.1-py2.py3-none-any.whl
Collecting py4j==0.10.9.7 (from pyspark)
  Obtaining dependency information for py4j==0.10.9.7 from https://files.pythonhosted.org/packages/10/30/a58b32568f1623aaad7db22aa9eafc4c6c194b429ff35bdc55ca2726da47/py4j-0.10.9.7-py2.py3-none-any.whl.metadata
  Using cached py4j-0.10.9.7-py2.py3-none-any.whl.metadata (1.5 kB)
Using cached py4j-0.10.9.7-py2.py3-none-any.whl (200 kB)
Installing collected packages: py4j, pyspark
Successfully installed py4j-0.10.9.7 pyspark-3.5.1

```

### kod do uruchomienia
```
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.show()
Test that writing works as well:

df.write.parquet('zones')
```

```
➜  jupyter_notebook python3 first_script.py
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/03/03 16:59:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/03/03 16:59:31 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
24/03/03 16:59:31 WARN Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
+----------+-------------+--------------------+------------+
|LocationID|      Borough|                Zone|service_zone|
+----------+-------------+--------------------+------------+
|         1|          EWR|      Newark Airport|         EWR|
|         2|       Queens|         Jamaica Bay|   Boro Zone|
|         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
|         4|    Manhattan|       Alphabet City| Yellow Zone|
|         5|Staten Island|       Arden Heights|   Boro Zone|
|         6|Staten Island|Arrochar/Fort Wad...|   Boro Zone|
|         7|       Queens|             Astoria|   Boro Zone|
|         8|       Queens|        Astoria Park|   Boro Zone|
|         9|       Queens|          Auburndale|   Boro Zone|
|        10|       Queens|        Baisley Park|   Boro Zone|
|        11|     Brooklyn|          Bath Beach|   Boro Zone|
|        12|    Manhattan|        Battery Park| Yellow Zone|
|        13|    Manhattan|   Battery Park City| Yellow Zone|
|        14|     Brooklyn|           Bay Ridge|   Boro Zone|
|        15|       Queens|Bay Terrace/Fort ...|   Boro Zone|
|        16|       Queens|             Bayside|   Boro Zone|
|        17|     Brooklyn|             Bedford|   Boro Zone|
|        18|        Bronx|        Bedford Park|   Boro Zone|
|        19|       Queens|           Bellerose|   Boro Zone|
|        20|        Bronx|             Belmont|   Boro Zone|
+----------+-------------+--------------------+------------+
only showing top 20 rows
```


```
<!-- version on without environment -->
spar.version
'3.5.1'


<!-- version local with wget -->
➜  spark-3.3.2-bin-hadoop3 spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/03/03 17:15:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/03/03 17:15:24 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
24/03/03 17:15:24 WARN Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
Spark context Web UI available at http://pc:4042
Spark context available as 'sc' (master = local[*], app id = local-1709482524613).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 17.0.9)
Type in expressions to have them evaluated.
Type :help for more information.

scala> spark.version
res0: String = 3.3.2

```