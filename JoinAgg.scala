val emp = Seq((1,"Smith",-1,"2018","10","M",3000),(2,"Rose",1,"2010","20","M",4000),(3,"Williams",1,"2010","10","M",1000),(4,"Jones",2,"2005","10","F",2000),(5,"Brown",2,"2010","40","",-1),(6,"Brown",2,"2010","50","",-1))
emp: Seq[(Int, String, Int, String, String, String, Int)] = List((1,Smith,-1,2018,10,M,3000), (2,Rose,1,2010,20,M,4000),
scala> val emp = Seq((1,"Smith",-1,"2018","10","M",3000),(2,"Rose",1,"2010","20","M",4000),(3,"Williams",1,"2010","10","M",1000),(4,"Jones",2,"2005","10","F",2000),(5,"Brown",2,"2010","40","",-1),(6,"Brown",2,"2010","50","",-1))
emp: Seq[(Int, String, Int, String, String, String, Int)] = List((1,Smith,-1,2018,10,M,3000), (2,Rose,1,2010,20,M,4000), (3,Williams,1,2010,10,M,1000), (4,Jones,2,2005,10,F,2000), (5,Brown,2,2010,40,"",-1), (6,Brown,2,2010,50,"",-1))

scala> val empColumns = Seq("emp_id","name","superior_emp_id","year_ joined","emp_dept_id","gender","salary")
empColumns: Seq[String] = List(emp_id, name, superior_emp_id, year_ joined, emp_dept_id, gender, salary)

scala> import spark.sqlContext.implicits._
import spark.sqlContext.implicits._


scala> val empDF = emp.toDF(empColumns:_*)
empDF: org.apache.spark.sql.DataFrame = [emp_id: int, name: string ... 5 more fields]

scala> empDF.show(false)
+------+--------+---------------+------------+-----------+------+------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|
+------+--------+---------------+------------+-----------+------+------+
|1     |Smith   |-1             |2018        |10         |M     |3000  |
|2     |Rose    |1              |2010        |20         |M     |4000  |
|3     |Williams|1              |2010        |10         |M     |1000  |
|4     |Jones   |2              |2005        |10         |F     |2000  |
|5     |Brown   |2              |2010        |40         |      |-1    |
|6     |Brown   |2              |2010        |50         |      |-1    |
+------+--------+---------------+------------+-----------+------+------+



scala> val dept = Seq(("Finance",10),("Marketing",20),("Sales",30),("IT",40))
dept: Seq[(String, Int)] = List((Finance,10), (Marketing,20), (Sales,30), (IT,40))



scala> val deptColumns = Seq("dept_name","dept_id")
deptColumns: Seq[String] = List(dept_name, dept_id)

scala> val deptDF = dept.toDF(deptColumns:_*)
deptDF: org.apache.spark.sql.DataFrame = [dept_name: string, dept_id: int]


scala> deptDF.show(false) ^
+---------+-------+
|dept_name|dept_id|
+---------+-------+
|Finance  |10     |
|Marketing|20     |
|Sales    |30     |
|IT       |40     |
+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"inner").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
+------+--------+---------------+------------+-----------+------+------+---------+-------+



scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"outer").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|6     |Brown   |2              |2010        |50         |      |-1    |null     |null   |
|null  |null    |null           |null        |null       |null  |null  |Sales    |30     |
+------+--------+---------------+------------+-----------+------+------+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"full").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|6     |Brown   |2              |2010        |50         |      |-1    |null     |null   |
|null  |null    |null           |null        |null       |null  |null  |Sales    |30     |
+------+--------+---------------+------------+-----------+------+------+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"fullouter").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|6     |Brown   |2              |2010        |50         |      |-1    |null     |null   |
|null  |null    |null           |null        |null       |null  |null  |Sales    |30     |
+------+--------+---------------+------------+-----------+------+------+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"left").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
|6     |Brown   |2              |2010        |50         |      |-1    |null     |null   |
+------+--------+---------------+------------+-----------+------+------+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"leftouter").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
|6     |Brown   |2              |2010        |50         |      |-1    |null     |null   |
+------+--------+---------------+------------+-----------+------+------+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"right").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|null  |null    |null           |null        |null       |null  |null  |Sales    |30     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
+------+--------+---------------+------------+-----------+------+------+---------+-------+


scala> empDF.join(deptDF,empDF("emp_dept_id") ===deptDF("dept_id"),"rightouter").show(false)
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_ joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+------------+-----------+------+------+---------+-------+
|4     |Jones   |2              |2005        |10         |F     |2000  |Finance  |10     |
|3     |Williams|1              |2010        |10         |M     |1000  |Finance  |10     |
|1     |Smith   |-1             |2018        |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010        |20         |M     |4000  |Marketing|20     |
|null  |null    |null           |null        |null       |null  |null  |Sales    |30     |
|5     |Brown   |2              |2010        |40         |      |-1    |IT       |40     |
+------+--------+---------------+------------+-----------+------+------+---------+-------+




scala> val simpleData = Seq(("James","Sales","NY",90000,34,10000),
     |     ("Michael","Sales","NY",86000,56,20000),
     |     ("Robert","Sales","CA",81000,30,23000),
     |     ("Maria","Finance","CA",90000,24,23000),
     |     ("Raman","Finance","CA",99000,40,24000),
     |     ("Scott","Finance","NY",83000,36,19000),
     |     ("Jen","Finance","NY",79000,53,15000),
     |     ("Jeff","Marketing","CA",80000,25,18000),
     |     ("Kumar","Marketing","NY",91000,50,21000)
     |   )
simpleData: Seq[(String, String, String, Int, Int, Int)] = List((James,Sales,NY,90000,34,10000), (Michael,Sales,NY,86000,56,20000), (Robert,Sales,CA,81000,30,23000), (Maria,Finance,CA,90000,24,23000), (Raman,Finance,CA,99000,40,24000), (Scott,Finance,NY,83000,36,19000), (Jen,Finance,NY,79000,53,15000), (Jeff,Marketing,CA,80000,25,18000), (Kumar,Marketing,NY,91000,50,21000))

scala> val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
df: org.apache.spark.sql.DataFrame = [employee_name: string, department: string ... 4 more fields]

scala> df.show()
+-------------+----------+-----+------+---+-----+
|employee_name|department|state|salary|age|bonus|
+-------------+----------+-----+------+---+-----+
|        James|     Sales|   NY| 90000| 34|10000|
|      Michael|     Sales|   NY| 86000| 56|20000|
|       Robert|     Sales|   CA| 81000| 30|23000|
|        Maria|   Finance|   CA| 90000| 24|23000|
|        Raman|   Finance|   CA| 99000| 40|24000|
|        Scott|   Finance|   NY| 83000| 36|19000|
|          Jen|   Finance|   NY| 79000| 53|15000|
|         Jeff| Marketing|   CA| 80000| 25|18000|
|        Kumar| Marketing|   NY| 91000| 50|21000|
+-------------+----------+-----+------+---+-----+


scala> val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
df: org.apache.spark.sql.DataFrame = [employee_name: string, department: string ... 4 more fields]

scala> df.show()
+-------------+----------+-----+------+---+-----+
|employee_name|department|state|salary|age|bonus|
+-------------+----------+-----+------+---+-----+
|        James|     Sales|   NY| 90000| 34|10000|
|      Michael|     Sales|   NY| 86000| 56|20000|
|       Robert|     Sales|   CA| 81000| 30|23000|
|        Maria|   Finance|   CA| 90000| 24|23000|
|        Raman|   Finance|   CA| 99000| 40|24000|
|        Scott|   Finance|   NY| 83000| 36|19000|
|          Jen|   Finance|   NY| 79000| 53|15000|
|         Jeff| Marketing|   CA| 80000| 25|18000|
|        Kumar| Marketing|   NY| 91000| 50|21000|
+-------------+----------+-----+------+---+-----+


scala> df.groupBy("department").sum("salary").show(false)
+----------+-----------+
|department|sum(salary)|
+----------+-----------+
|Sales     |257000     |
|Finance   |351000     |
|Marketing |171000     |
+----------+-----------+



scala> df.groupBy("department").count().show()
+----------+-----+
|department|count|
+----------+-----+
|     Sales|    3|
|   Finance|    4|
| Marketing|    2|
+----------+-----+



scala> df.groupBy("department").min("salary").show()
+----------+-----------+
|department|min(salary)|
+----------+-----------+
|     Sales|      81000|
|   Finance|      79000|
| Marketing|      80000|
+----------+-----------+


scala> df.groupBy("department").max("salary").show()
+----------+-----------+
|department|max(salary)|
+----------+-----------+
|     Sales|      90000|
|   Finance|      99000|
| Marketing|      91000|
+----------+-----------+


scala> df.groupBy("department").avg( "salary").show()
+----------+-----------------+
|department|      avg(salary)|
+----------+-----------------+
|     Sales|85666.66666666667|
|   Finance|          87750.0|
| Marketing|          85500.0|
+----------+-----------------+


scala> df.groupBy("department").mean( "salary").show()
+----------+-----------------+
|department|      avg(salary)|
+----------+-----------------+
|     Sales|85666.66666666667|
|   Finance|          87750.0|
| Marketing|          85500.0|
+----------+-----------------+


scala> df.groupBy("department","state")
res25: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [department: string, state: string], value: [employee_name: string, department: string ... 4 more fields], type: GroupBy]

scala>     .sum("salary","bonus")
res26: org.apache.spark.sql.DataFrame = [department: string, state: string ... 2 more fields]

scala>     .show(false)
+----------+-----+-----------+----------+
|department|state|sum(salary)|sum(bonus)|
+----------+-----+-----------+----------+
|Finance   |NY   |162000     |34000     |
|Marketing |NY   |91000      |21000     |
|Sales     |CA   |81000      |23000     |
|Marketing |CA   |80000      |18000     |
|Finance   |CA   |189000     |47000     |
|Sales     |NY   |176000     |30000     |
+----------+-----+-----------+----------+


scala> df.groupBy("department")
res28: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [department: string], value: [employee_name: string, department: string ... 4 more fields], type: GroupBy]

scala>     .agg(
     |       sum("salary").as("sum_salary"),
     |       avg("salary").as("avg_salary"),
     |       sum("bonus").as("sum_bonus"),
     |       max("bonus").as("max_bonus"))
res29: org.apache.spark.sql.DataFrame = [department: string, sum_salary: bigint ... 3 more fields]

scala>     .show(false)
+----------+----------+-----------------+---------+---------+
|department|sum_salary|avg_salary       |sum_bonus|max_bonus|
+----------+----------+-----------------+---------+---------+
|Sales     |257000    |85666.66666666667|53000    |23000    |
|Finance   |351000    |87750.0          |81000    |24000    |
|Marketing |171000    |85500.0          |39000    |21000    |
+----------+----------+-----------------+---------+---------+


scala> df.groupBy("department")
res31: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [department: string], value: [employee_name: string, department: string ... 4 more fields], type: GroupBy]

scala>     .agg(
     |       sum("salary").as("sum_salary"),
     |       avg("salary").as("avg_salary"),
     |       sum("bonus").as("sum_bonus"),
     |       max("bonus").as("max_bonus"))
res32: org.apache.spark.sql.DataFrame = [department: string, sum_salary: bigint ... 3 more fields]

scala>     .where(col("sum_bonus") >= 50000)
res33: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [department: string, sum_salary: bigint ... 3 more fields]

scala>     .show(false)
+----------+----------+-----------------+---------+---------+
|department|sum_salary|avg_salary       |sum_bonus|max_bonus|
+----------+----------+-----------------+---------+---------+
|Sales     |257000    |85666.66666666667|53000    |23000    |
|Finance   |351000    |87750.0          |81000    |24000    |
+----------+----------+-----------------+---------+---------+


val goalsDF = Seq(
     |   ("messi", 2),
     |   ("messi", 1),
     |   ("pele", 3),
     |   ("pele", 1)
     | ).toDF("name", "goals")
goalsDF: org.apache.spark.sql.DataFrame = [name: string, goals: int]

scala> goalsDF.show()
+-----+-----+
| name|goals|
+-----+-----+
|messi|    2|
|messi|    1|
| pele|    3|
| pele|    1|
+-----+-----+


scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> goalsDF
res1: org.apache.spark.sql.DataFrame = [name: string, goals: int]

scala>   .groupBy("name")
res2: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [name: string], value: [name: string, goals: int], type: GroupBy]

scala>   .agg(sum("goals"))
res3: org.apache.spark.sql.DataFrame = [name: string, sum(goals): bigint]

scala>   .show()
+-----+----------+
| name|sum(goals)|
+-----+----------+
| pele|         4|
|messi|         3|
+-----+----------+




scala> goalsDF
res5: org.apache.spark.sql.DataFrame = [name: string, goals: int]

scala>   .groupBy("name")
res6: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [name: string], value: [name: string, goals: int], type: GroupBy]

scala>   .sum()
res7: org.apache.spark.sql.DataFrame = [name: string, sum(goals): bigint]

scala>   .show()
+-----+----------+
| name|sum(goals)|
+-----+----------+
| pele|         4|
|messi|         3|
+-----+----------+


scala> val studentsDF = Seq(
     |   ("mario", "italy", "europe"),
     |   ("stefano", "italy", "europe"),
     |   ("victor", "spain", "europe"),
     |   ("li", "china", "asia"),
     |   ("yuki", "japan", "asia"),
     |   ("vito", "italy", "europe")
     | ).toDF("name", "country", "continent")
studentsDF: org.apache.spark.sql.DataFrame = [name: string, country: string ... 1 more field]

scala> studentsDF
res9: org.apache.spark.sql.DataFrame = [name: string, country: string ... 1 more field]

scala>   .groupBy("continent", "country")
res10: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [continent: string, country: string], value: [name: string, country: string ... 1 more field], type: GroupBy]

scala>   .agg(count("*"))
res11: org.apache.spark.sql.DataFrame = [continent: string, country: string ... 1 more field]

scala>   .show()
+---------+-------+--------+
|continent|country|count(1)|
+---------+-------+--------+
|   europe|  italy|       3|
|     asia|  japan|       1|
|   europe|  spain|       1|
|     asia|  china|       1|
+---------+-------+--------+


scala>

scala> studentsDF
res13: org.apache.spark.sql.DataFrame = [name: string, country: string ... 1 more field]

scala>   .groupBy("continent", "country")
res14: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [continent: string, country: string], value: [name: string, country: string ... 1 more field], type: GroupBy]

scala>   .count()
res15: org.apache.spark.sql.DataFrame = [continent: string, country: string ... 1 more field]

scala>   .show()
+---------+-------+-----+
|continent|country|count|
+---------+-------+-----+
|   europe|  italy|    3|
|     asia|  japan|    1|
|   europe|  spain|    1|
|     asia|  china|    1|
+---------+-------+-----+


scala> val hockeyPlayersDF = Seq(
     |   ("gretzky", 40, 102, 1990),
     |   ("gretzky", 41, 122, 1991),
     |   ("gretzky", 31, 90, 1992),
     |   ("messier", 33, 61, 1989),
     |   ("messier", 45, 84, 1991),
     |   ("messier", 35, 72, 1992),
     |   ("messier", 25, 66, 1993)
     | ).toDF("name", "goals", "assists", "season")
hockeyPlayersDF: org.apache.spark.sql.DataFrame = [name: string, goals: int ... 2 more fields]

scala> hockeyPlayersDF
res17: org.apache.spark.sql.DataFrame = [name: string, goals: int ... 2 more fields]

scala>   .where($"season".isin("1991", "1992"))
res18: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [name: string, goals: int ... 2 more fields]

scala>   .groupBy("name")
res19: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [name: string], value: [name: string, goals: int ... 2 more fields], type: GroupBy]

scala>   .agg(avg("goals"), avg("assists"))
res20: org.apache.spark.sql.DataFrame = [name: string, avg(goals): double ... 1 more field]

scala>   .show()
+-------+----------+------------+
|   name|avg(goals)|avg(assists)|
+-------+----------+------------+
|messier|      40.0|        78.0|
|gretzky|      36.0|       106.0|
+-------+----------+------------+


scala> hockeyPlayersDF
res22: org.apache.spark.sql.DataFrame = [name: string, goals: int ... 2 more fields]

scala>   .groupBy("name")
res23: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [name: string], value: [name: string, goals: int ... 2 more fields], type: GroupBy]

scala>   .agg(avg("goals"), avg("assists").as("average_assists"))
res24: org.apache.spark.sql.DataFrame = [name: string, avg(goals): double ... 1 more field]

scala>   .where($"average_assists" >= 100)
res25: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [name: string, avg(goals): double ... 1 more field]

scala>   .show()
+-------+------------------+------------------+
|   name|        avg(goals)|   average_assists|
+-------+------------------+------------------+
|gretzky|37.333333333333336|104.66666666666667|
+-------+------------------+------------------+


s