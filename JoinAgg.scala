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


