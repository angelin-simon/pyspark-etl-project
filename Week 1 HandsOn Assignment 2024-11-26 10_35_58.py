WEEK 1 HANDS ON ASSESSMENT 1

da_df=spark.read.load('dbfs:/FileStore/tables/NYSE_daily.tsv',format='csv',delimiter='\t',inferSchema=True)


dv_df=spark.read.load("dbfs:/FileStore/tables/NYSE_dividends.tsv",format='csv',delimiter='\t',inferSchema=True)


da_df.show()


da=da_df.withColumnRenamed("_c0","exchange").withColumnRenamed("_c1","stockSymbol").withColumnRenamed("_c2","date").withColumnRenamed("_c3","openDate").withColumnRenamed("_c4","highPrice").withColumnRenamed("_c5","lowPrice").withColumnRenamed("_c6","closePrice").withColumnRenamed("_c7","volume").withColumnRenamed("_c8","adjustedClosePrice")


dv=dv_df.withColumnRenamed("_c0","exchange").withColumnRenamed("_c1","stockSymbol").withColumnRenamed("_c2","date").withColumnRenamed("_c3","dividends")


da.show()


dv.show()


da.createOrReplaceTempView("Daily_nyse")



dv.createOrReplaceTempView("Dividends_nyse")


1.
p1=spark.sql("SELECT distinct stockSymbol from Daily_nyse where closePrice>200 and volume>=10000000")


p1.show()


+-----------+
|stockSymbol|
+-----------+
|       JNPR|
+-----------+


2.
p2=spark.sql("select stockSymbol,count(dividends) COUNT from dividends_nyse group by stockSymbol having COUNT>50")


p2.show()

+-----------+-----+
|stockSymbol|COUNT|
+-----------+-----+
|        JCP|  114|
|        JEF|   72|
|        JPM|  104|
|        JRO|   63|
|        JFP|   58|
|        JHI|   99|
|        JNJ|  160|
|        JHS|   88|
|        JTP|   91|
|        JOE|   51|
|        JQC|   55|
|        JHP|   85|
|        JPS|   89|
|        JPC|   60|
|        JCI|   97|
|        JFR|   68|
|        JWN|   81|
+-----------+-----+




3.
p3=spark.sql("""select d.stockSymbol, d.closePrice, v.dividends, d.date
    from Daily_nyse d
    join Dividends_nyse v 
    on d.stockSymbol = v.stockSymbol and d.date = v.date
    where d.closePrice>=100 and v.dividends > 0.01
    order by v.dividends""")


p3.show()

+-----------+----------+---------+----------+
|stockSymbol|closePrice|dividends|      date|
+-----------+----------+---------+----------+
|        JNJ|    101.62|  0.02625|1987-08-17|
|        JNJ|    104.37|     0.05|1992-02-11|
|        JCI|    109.55|    0.075|2003-12-10|
|        JCI|    107.34|     0.11|2007-09-12|
|        JCI|    110.87|     0.11|2007-06-13|
|        JNJ|    103.75|     0.14|1999-11-12|
|        JNJ|     101.0|     0.18|2001-05-18|
|        JPM|    110.62|  0.20667|1998-01-02|
|        JPM|    120.44|  0.20667|1997-10-02|
|        JPM|    139.31|     0.24|1998-04-02|
|        JLL|    117.09|     0.35|2007-05-11|
+-----------+----------+---------+----------+


4.
p1.write.csv("dbfs:/user/tables/result1_week1_HO.csv")


p2.write.csv("dbfs:/user/tables/result2_week1_HO.csv")


p3.write.csv("dbfs:/user/tables/result3_week1_HO.csv")




