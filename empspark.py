from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Employee Salary Analysis").getOrCreate()

# Sample DataFrame
data = [
    ('John Doe', '1990-05-15', '2010-01-01', 50000, 'Engineering'),
    ('Jane Smith', '1985-07-20', '2008-03-15', 75000, 'HR'),
    ('Mike Johnson', '1970-10-10', '1995-10-20', 95000, 'Finance'),
    ('Emily Davis', '1995-12-25', '2020-06-10', 60000, 'Engineering'),
    ('Robert Brown', '1980-11-05', '2005-07-25', 85000, 'Finance')
]

columns = ['name', 'date_of_birth', 'date_of_joining', 'salary', 'department']
df = spark.createDataFrame(data, columns)

# Function to get the employee(s) with the maximum salary
def get_max_salary_employee(df):
    max_salary = df.agg(F.max('salary')).collect()[0][0]
    return df.filter(df['salary'] == max_salary)

# Function to get the employee(s) with the minimum salary
def get_min_salary_employee(df):
    min_salary = df.agg(F.min('salary')).collect()[0][0]
    return df.filter(df['salary'] == min_salary)

# Function to get the youngest employee
def get_youngest_employee(df):
    return df.orderBy(F.desc('date_of_birth')).limit(1)

# Function to get the senior-most employee
def get_senior_employee(df):
    return df.orderBy(F.asc('date_of_joining')).limit(1)

# Function to get the department with the highest average salary
def get_department_with_highest_avg_salary(df):
    department_avg_salary = df.groupBy('department').agg(F.avg('salary').alias('avg_salary'))
    max_avg_salary = department_avg_salary.agg(F.max('avg_salary')).collect()[0][0]
    return department_avg_salary.filter(department_avg_salary['avg_salary'] == max_avg_salary)

# Calling the functions
max_salary_employee = get_max_salary_employee(df)
min_salary_employee = get_min_salary_employee(df)
youngest_employee = get_youngest_employee(df)
senior_employee = get_senior_employee(df)
department_with_highest_avg_salary = get_department_with_highest_avg_salary(df)

# Display the results
print("Employee(s) with the Maximum Salary:")
max_salary_employee.show()

print("Employee(s) with the Minimum Salary:")
min_salary_employee.show()

print("Youngest Employee:")
youngest_employee.show()

print("Senior-most Employee:")
senior_employee.show()

print("Department with the Highest Average Salary:")
department_with_highest_avg_salary.show()
