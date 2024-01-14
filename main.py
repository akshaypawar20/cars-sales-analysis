from lib.tasks.task2 import processing_two
from lib.tasks.task3 import processing_three
from lib.tasks.task4 import processing_four
from lib.utils import create_spark_session, read_file
from conf.Variables import FILE_PATH, CSV
from lib.tasks.task1 import processing_one

if __name__ == "__main__":
    spark = create_spark_session()
    df = read_file(spark, CSV, FILE_PATH)
    task_1 = processing_one(df)
    task_2 = processing_two(df)
    task_3 = processing_three(df)
    task_4 = processing_four(df)