Windows PowerShell


PS C:\Users\user> spark-shell


scala> import spark.implicits._
import spark.implicits._

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

scala>

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


scala>

scala> df.groupBy("department").sum("salary").show(false)
+----------+-----------+
|department|sum(salary)|
+----------+-----------+
|Sales     |257000     |
|Finance   |351000     |
|Marketing |171000     |
+----------+-----------+


scala>

scala> df.groupBy("department").min("salary").show()
+----------+-----------+
|department|min(salary)|
+----------+-----------+
|     Sales|      81000|
|   Finance|      79000|
| Marketing|      80000|
+----------+-----------+


scala>

scala> df.groupBy("department").avg( "salary").show()
+----------+-----------------+
|department|      avg(salary)|
+----------+-----------------+
|     Sales|85666.66666666667|
|   Finance|          87750.0|
| Marketing|          85500.0|
+----------+-----------------+


scala>

scala> df.groupBy("department").mean( "salary")
res4: org.apache.spark.sql.DataFrame = [department: string, avg(salary): double]

scala>

scala> df.groupBy("department").mean( "salary").show()
+----------+-----------------+
|department|      avg(salary)|
+----------+-----------------+
|     Sales|85666.66666666667|
|   Finance|          87750.0|
| Marketing|          85500.0|
+----------+-----------------+


scala>

scala> df.groupBy("department","state").sum("salary","bonus").show(false)
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


scala>

scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala>

scala> df.groupBy("department")
res7: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [department: string], value: [employee_name: string, department: string ... 4 more fields], type: GroupBy]

scala>     .agg(
     |       sum("salary").as("sum_salary"),
     |       avg("salary").as("avg_salary"),
     |       sum("bonus").as("sum_bonus"),
     |       max("bonus").as("max_bonus"))
res8: org.apache.spark.sql.DataFrame = [department: string, sum_salary: bigint ... 3 more fields]

scala>     .show(false)
+----------+----------+-----------------+---------+---------+
|department|sum_salary|avg_salary       |sum_bonus|max_bonus|
+----------+----------+-----------------+---------+---------+
|Sales     |257000    |85666.66666666667|53000    |23000    |
|Finance   |351000    |87750.0          |81000    |24000    |
|Marketing |171000    |85500.0          |39000    |21000    |
+----------+----------+-----------------+---------+---------+


scala> df.groupBy("department")
res10: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [department: string], value: [employee_name: string, department: string ... 4 more fields], type: GroupBy]

scala>     .agg(
     |       sum("salary").as("sum_salary"),
     |       avg("salary").as("avg_salary"),
     |       sum("bonus").as("sum_bonus"),
     |       max("bonus").as("max_bonus"))
res11: org.apache.spark.sql.DataFrame = [department: string, sum_salary: bigint ... 3 more fields]

scala>     .where(col("sum_bonus") >= 50000)
res12: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [department: string, sum_salary: bigint ... 3 more fields]

scala>     .show(false)
+----------+----------+-----------------+---------+---------+
|department|sum_salary|avg_salary       |sum_bonus|max_bonus|
+----------+----------+-----------------+---------+---------+
|Sales     |257000    |85666.66666666667|53000    |23000    |
|Finance   |351000    |87750.0          |81000    |24000    |
+----------+----------+-----------------+---------+---------+


