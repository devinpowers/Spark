from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("IrisSLMeans")
sc = SparkContext(conf=conf)


def parse_input(line: str) -> tuple:
    cols = line.split(",")
    sepal_length = float(cols[0])
    species = str(cols[-1])
    return (species, sepal_length)


if __name__ == "__main__":
    
    '''Run Script '''
    # 1. Read the text file
    iris = sc.textFile("file:///Users/devinpowers/Desktop/Hadoop/spark-and-python-for-big-data-with-pyspark-master/iris.csv")
    
    # 2. Remove the header row
    iris_header = iris.first()
    iris_header = sc.parallelize([iris_header])
    iris = iris.subtract(iris_header)
    
    # 3. Parse the input
    iris_parsed = iris.map(parse_input)
    
    # 4. Calculate totals - sum of all sepal_length values per flower species
    iris_sl_totals = iris_parsed.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    
    # 5. Calculate means - Divide the total by the number of instances
    iris_sl_means = iris_sl_totals.mapValues(lambda x: round(x[0] / x[1], 2))
    
    # 6. Wrap into a result
    result = iris_sl_means.collect()
    
    # Print
    for val in result:
        print(f"Iris species {val[0]} has an average sepal length of {val[1]}")