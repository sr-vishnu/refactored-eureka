

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType



object LogParser {
  def main(args: Array[String]) {

    
    val spark = SparkSession.builder.appName("LogParser").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    val INPUT = args(0)
    val OUTPUT = args(1)

    def load() : DataFrame = {
      val raw_df: DataFrame = spark.read.text(INPUT)
      return raw_df;
    }

    def write(df: DataFrame)  = {
      df.write.mode("append").json(OUTPUT)
    }

    def preProcess(df: DataFrame) : DataFrame = {
      val rules: Seq[String] = Seq(
        """.*://omwssu.*""",
      )
      return rules.foldLeft(df){
        (acc_df,rule) => acc_df.filter(
          col("value").rlike(rule)
        )
      }
    }

    /*Method to do transformations on a given column*/
    def process(df: DataFrame, column: String): DataFrame = {

      /*A mapping between column name to regex to be used for processing*/
      val columntoregex: Map[String, String] = Map(
        "message" -> """.*(http://[^\s]+).*""",
        "fqdn" -> """.*(http:/(?:/[^\s/]+){2}).*""",
        "cpe_id" -> """.*://(?:[^\s/]+/){2}([^\s/]+)/.*""",
        "action" -> """.*://(?:[^\s/]+/){3}([^\s/]+)/.*""",
        "error_code" -> """.*://(?:[^\s/]+/){4}(\d+/\d+).*""",
        "timestamp" -> """.*(\d{4}(?:-\d{2}){2}\s+\d{2}(?::\d{2})).*""",
      )

      def extractAndTransformErrorCode(): DataFrame = {
        val regex = columntoregex.getOrElse(column,"")
        return df.withColumn(
          column,regexp_replace(
            regexp_extract(col("value"), regex, 1),
            "/",
            "."
          ).cast(DoubleType)
        )
      }

      def extractAndTransformTimestamp(): DataFrame = {
        val regex = columntoregex.getOrElse(column, "")
        return df.withColumn(
          column,
          to_timestamp(
              regexp_replace(
                regexp_extract(
                  col("value"),regex,1),
                  "\\s+",
                  "T"
              )
          )
        )
      }

      def extractField(): DataFrame = {
        val regex = columntoregex.getOrElse(column, "")
        return df.withColumn(column, regexp_extract(col("value"), regex, 1))
      }

      /*A mapping from colum name to method to be used for processing*/
      val dispatcher: Map[String, () => DataFrame] = Map(
        "fqdn" -> extractField,
        "cpe_id" -> extractField,
        "action" -> extractField,
        "message" -> extractField,
        "timestamp" -> extractAndTransformTimestamp,
        "error_code" -> extractAndTransformErrorCode,
      )
      return dispatcher(column)()
    }

    /*Run the process method for all columns*/
    def processMultipleCols(df: DataFrame,columns: Seq[String]) : DataFrame = {
      return columns.foldLeft(df){
        (acc_df , column) => process(acc_df,column)
      }.drop("value")
    }

    /*##########################Driver Code start##############################################*/
    val fields = Seq(
      "timestamp",
      "cpe_id",
      "fqdn",
      "action",
      "error_code",
      "message"
    )
    val raw_df = load()
    val preprocessed_df = preProcess(raw_df)
    val processed_df = processMultipleCols(preprocessed_df,fields)
    processed_df.printSchema()
    processed_df.show()
    write(processed_df)
    /*############################Driver Code end#############################################*/

    spark.stop();
  }

}
