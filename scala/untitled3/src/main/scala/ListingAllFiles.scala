import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path, LocatedFileStatus, RemoteIterator}

object ListingAllFiles {
  def main(args: Array[String]): Unit = {

    val spark = Configurer.localSparkEnvironment("List all files from a bucket")

    val bucketPath = new Path("s3a://csv/")

    val fs = bucketPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val files: RemoteIterator[LocatedFileStatus] = fs.listFiles(bucketPath, true)

    println("Files in 'csv' bucket:")
    while (files.hasNext) {
      val file = files.next()
      println(s"- ${file.getPath.toString}  (size = ${file.getLen})")
    }

    spark.stop()
  }
}
