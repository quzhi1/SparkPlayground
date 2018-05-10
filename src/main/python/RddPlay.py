from operator import add

from pyspark import SparkContext

sc = SparkContext("local", "play RDD app")
# Distribute a local Python collection to form an RDD
words = sc.parallelize(
    ["scala",
     "java",
     "hadoop",
     "spark",
     "akka",
     "spark vs hadoop",
     "pyspark",
     "pyspark and spark"]
)

# Count
counts = words.count()
print("Number of elements in RDD -> %i" % counts)

# Collect
coll = words.collect()
print("Elements in RDD -> %s" % coll)


# foreach
def f(x): print("Print: " + x)


fore = words.foreach(f)

# filter
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("Filtered RDD -> %s" % filtered)

# Map
mapping = words.map(lambda word: (word, 1)).collect()
print("Key value pair -> %s" % mapping)

# Reduce
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print("Adding all the elements -> %i" % adding)

# Join
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print("Join RDD -> %s" % final)

# Cache
words.cache()
caching = words.persist().is_cached
print("Words got chached > %s" % caching)
