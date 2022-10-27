package dstransformations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SecondHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SecondHomeWork")
    .master("local")
    .getOrCreate()

  case class Employee(
                       employee_Name: String
                       , empID: Int
                       , marriedID: Int
                       , maritalStatusID: Int
                       , genderID: Int
                       , empStatusID: Int
                       , deptID: Int
                       , perfScoreID: Int
                       , fromDiversityJobFairID: Int
                       , salary: Int
                       , termd: Int
                       , positionID: Int
                       , position: String
                       , state: String
                       , zip: Int
                       , dob: String
                       , sex: String
                       , maritalDesc: String
                       , citizenDesc: String
                       , hispanicLatino: String
                       , raceDesc: String
                       , dateofHire: String
                       , dateofTermination: String
                       , termReason: String
                       , employmentStatus: String
                       , department: String
                       , managerName: String
                       , managerID: Option[Int]
                       , recruitmentSource: String
                       , performanceScore: String
                       , engagementSurvey: String
                       , empSatisfaction: Int
                       , specialProjectsCount: Int
                       , lastPerformanceReview_Date: String
                       , daysLateLast30: Int
                       , absences: Int
                     )

  val employeeSchema = StructType(Seq(
    StructField("Employee_Name", StringType)
    , StructField("EmpID", IntegerType)
    , StructField("MarriedID", IntegerType)
    , StructField("MaritalStatusID", IntegerType)
    , StructField("GenderID", IntegerType)
    , StructField("EmpStatusID", IntegerType)
    , StructField("DeptID", IntegerType)
    , StructField("PerfScoreID", IntegerType)
    , StructField("FromDiversityJobFairID", IntegerType)
    , StructField("Salary", IntegerType)
    , StructField("Termd", IntegerType)
    , StructField("PositionID", IntegerType)
    , StructField("Position", StringType)
    , StructField("State", StringType)
    , StructField("Zip", IntegerType)
    , StructField("DOB", StringType)
    , StructField("Sex", StringType)
    , StructField("MaritalDesc", StringType)
    , StructField("CitizenDesc", StringType)
    , StructField("HispanicLatino", StringType)
    , StructField("RaceDesc", StringType)
    , StructField("DateofHire", StringType)
    , StructField("DateofTermination", StringType)
    , StructField("TermReason", StringType)
    , StructField("EmploymentStatus", StringType)
    , StructField("Department", StringType)
    , StructField("ManagerName", StringType)
    , StructField("ManagerID", IntegerType)
    , StructField("RecruitmentSource", StringType)
    , StructField("PerformanceScore", StringType)
    , StructField("EngagementSurvey", StringType)
    , StructField("EmpSatisfaction", IntegerType)
    , StructField("SpecialProjectsCount", IntegerType)
    , StructField("LastPerformanceReview_Date", StringType)
    , StructField("DaysLateLast30", IntegerType)
    , StructField("Absences", IntegerType)
  ))

  import spark.implicits._

  val employeeDS = spark.read
    .option("header", "true")
    .schema(employeeSchema)
    .csv("src/main/resources/hrdataset.csv")
    .as[Employee]

    val positionQuery = List("BI", "IT")
//  val positionQuery = List("BI", "IT", "Pro", "it")

  case class Result(
                     positionID: Int
                     , position: String
                   )

  val queriesStack = mutable.Stack[String]()
  positionQuery.foreach(s => queriesStack.addOne(s))

  val DsList = getListOfDSs(queriesStack)

  def getListOfDSs(stack: mutable.Stack[String]) = {
    var ds = new ListBuffer[Dataset[SecondHomeWork.Result]]()

    while (stack.nonEmpty) {
      val t = getStringFromStack(stack)
      if (!t.equals(None)) {
        var x = employeeDS
          .filter(s => s.position.contains(t.toString))
          .map(emp => Result(emp.positionID, emp.position))
          .distinct()
        ds += x
      } else {
        ds :: Nil
      }
    }
    ds
  }

  def getStringFromStack(stack: mutable.Stack[String]) = {
    var s: Any = ""
    if (stack.nonEmpty) {
      s = stack.pop()
    }
    else {
      s = None
    }
    s
  }

  var resultDS: Dataset[SecondHomeWork.Result] = employeeDS
    .filter(emp => emp.position.contains("None"))
    .map(emp => Result(emp.positionID, emp.position))

  val countingResultDS: Dataset[SecondHomeWork.Result] = {
    var res: Dataset[SecondHomeWork.Result] =
      employeeDS
        .filter(emp => emp.position.contains("None"))
        .map(emp => Result(emp.positionID, emp.position))

    while (DsList.nonEmpty) {
      val k = DsList.remove(0)
      resultDS = res.union(k)
      res = resultDS
    }
    res
  }

  resultDS.show(false)
  spark.stop()
}
