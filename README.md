# GitHub Repositories Big Data Analysis

* Project about the Big Data analysis of GitHub Repositories in real time. 
* Using Apache Spark Streaming, Python and SQL, I analyzed the data of the most recently pushed repositories in real time to see which programming language was most popular as the primary language among recently pushed repositories. 
* I used the Docker environment and Apache Spark Streaming, to observe the live stream of thousands of the most recently pushed repositories.
* The aim of the project was to count the number of unique repositories that used any of Python, Java or Ruby as primary language, the average star_gazers count per language and the most popular words in the description of all unique repositories per language. 
* I used several big data analytics tools like MapReduce, Docker, Pandas, PySpark, and Matplotlib. 
* I stored the data in Resilient Distributed Datasets in sets of 50.
    * then I transformed and manipulated the data using pyspark commands.
    * Displayed real time results in batches and tabular forms in the Command Line Interface using SQL
    * I also used Flask to build a web application dashboard to display my results in the form of bar and line charts in real time. 
