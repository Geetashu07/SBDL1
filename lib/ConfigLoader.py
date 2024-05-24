#Here we are creating 3 Functions
import configparser
from pyspark import SparkConf


# This will read sbdl.conf file from conf directory, place all the configuration in python dictionary
#The idea is to read SBDL conf at once at the start of the applicatoin and keep it in memory so that we can use it whereever need it.
# We do now want to read this config again and again, so we will keep them in in-memory.
#It is take current environemnt as an input (local, QA, prod).The dunction will read the appropriate section from the sbdl.conf acc to current env.
# If we are running in QA it will going to keep all the QA config in dictionary.
def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf

# This is also similar to get_conf function.  This will create a spark conf object and add all the configuration in the spark_conf
# We are not keeping the conf values in python dictionary. Instead we are keeping it in spark conf object.
# Because spark_conf are used only once at the time of create system.
# And spark section will directly use this conf object.

def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf

#This will help to build a where clause wat run time.
#We will be loading data from HIVE table but  we want the have the felxibility to configure filter condition.
# This basically we create a string and we will use this while loading the data using where condition.
def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
