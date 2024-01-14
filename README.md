# Project Title

Car Sales Analysis using PySpark -> An Exploratory Data Analysis Project for the Car Sales Data using various PySpark Transformations.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. Please note that this is for Windows Machine for Mac/Linux users. The prerequisites would be similar with some differences, so check that out.

### Pre-Requisites

What things you need to install the software,

```
1. JDK 11 (Please check the JDK Version Compatible with Spark version)
2. Python 3.12.1 or latest
3. spark 3.5.0 or latest
4. winutils master (check which version is compatible with spark and create a ENV variable accordingly, for e.g with spark 3.5.0, hadoop 3.3.5 works)
5. PyCharm or your favourite IDE
6. pyspark package
```

### Project Structure
1. Conf -> It has all the configurations parameters </br>
2. Data -> Cars Sales data csv file used in this project </br>
3. Lib &nbsp;&nbsp; -> It has two main things, </br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;a. Tasks Folder -> It has all the functions implemented </br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;b. utils.py &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-> Utility functions like, Create a spark Session, Read Dataframe, Write DataFrame is stored here </br>
5. main.py (An entrypoint to all the functionalities within the project)
