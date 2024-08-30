import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from test.TestUtils import TestUtils
from empspark import *
class EmployeeSalaryAnalysisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark Session
        cls.spark = SparkSession.builder.appName("Employee Salary Analysis Test").getOrCreate()

        # Sample DataFrame
        data = [
            ('John Doe', '1990-05-15', '2010-01-01', 50000, 'Engineering'),
            ('Jane Smith', '1985-07-20', '2008-03-15', 75000, 'HR'),
            ('Mike Johnson', '1970-10-10', '1995-10-20', 95000, 'Finance'),
            ('Emily Davis', '1995-12-25', '2020-06-10', 60000, 'Engineering'),
            ('Robert Brown', '1980-11-05', '2005-07-25', 85000, 'Finance')
        ]

        columns = ['name', 'date_of_birth', 'date_of_joining', 'salary', 'department']
        cls.df = cls.spark.createDataFrame(data, columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_max_salary_employee(self):
        max_salary_employee = get_max_salary_employee(self.df)
        expected_name = 'Mike Johnson'

        if max_salary_employee.collect()[0]['name'] == expected_name:
            TestUtils.yakshaAssert("TestGetMaxSalaryEmployee", True, "boundary")
            print("TestGetMaxSalaryEmployee = Passed")
        else:
            TestUtils.yakshaAssert("TestGetMaxSalaryEmployee", False, "boundary")
            print("TestGetMaxSalaryEmployee = Failed")

    def test_get_min_salary_employee(self):
        min_salary_employee = get_min_salary_employee(self.df)
        expected_name = 'John Doe'

        if min_salary_employee.collect()[0]['name'] == expected_name:
            TestUtils.yakshaAssert("TestGetMinSalaryEmployee", True, "boundary")
            print("TestGetMinSalaryEmployee = Passed")
        else:
            TestUtils.yakshaAssert("TestGetMinSalaryEmployee", False, "boundary")
            print("TestGetMinSalaryEmployee = Failed")

    def test_get_youngest_employee(self):
        youngest_employee = get_youngest_employee(self.df)
        expected_name = 'Emily Davis'

        if youngest_employee.collect()[0]['name'] == expected_name:
            TestUtils.yakshaAssert("TestGetYoungestEmployee", True, "boundary")
            print("TestGetYoungestEmployee = Passed")
        else:
            TestUtils.yakshaAssert("TestGetYoungestEmployee", False, "boundary")
            print("TestGetYoungestEmployee = Failed")

    def test_get_senior_employee(self):
        senior_employee = get_senior_employee(self.df)
        expected_name = 'Mike Johnson'

        if senior_employee.collect()[0]['name'] == expected_name:
            TestUtils.yakshaAssert("TestGetSeniorEmployee", True, "boundary")
            print("TestGetSeniorEmployee = Passed")
        else:
            TestUtils.yakshaAssert("TestGetSeniorEmployee", False, "boundary")
            print("TestGetSeniorEmployee = Failed")

    def test_get_department_with_highest_avg_salary(self):
        department_with_highest_avg_salary = get_department_with_highest_avg_salary(self.df)
        expected_department = 'Finance'

        if department_with_highest_avg_salary.collect()[0]['department'] == expected_department:
            TestUtils.yakshaAssert("TestGetDepartmentWithHighestAvgSalary", True, "boundary")
            print("TestGetDepartmentWithHighestAvgSalary = Passed")
        else:
            TestUtils.yakshaAssert("TestGetDepartmentWithHighestAvgSalary", False, "boundary")
            print("TestGetDepartmentWithHighestAvgSalary = Failed")

if __name__ == '__main__':
    unittest.main()
