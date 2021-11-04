/*
 *
 *  Copyright (c) 2021. Mark Grechanik and Lone Star Consulting, Inc. All rights reserved.
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under
 *   the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 *
 */
import Generation.{LogMsgSimulator, RandomStringGenerator}
import HelperUtils.{CreateLogger, ObtainConfigReference, Parameters}

import java.io.File
import collection.JavaConverters.*
import scala.concurrent.{Await, Future, duration}
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3ClientBuilder}
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.regions.Regions
import com.amazonaws.services.rekognition.model.RegionOfInterest
import com.typesafe.config.ConfigFactory

object GenerateLogData:
  val logger = CreateLogger(classOf[GenerateLogData.type])

  //this is the main starting point for the log generator
  @main def runLogGenerator =
    import Generation.RSGStateMachine.*
    import Generation.*
    import HelperUtils.Parameters.*
    import GenerateLogData.*
    val config = ConfigFactory.load()
    logger.info("Log data generator started...")
    val INITSTRING = "Starting the string generation"
    val init = unit(INITSTRING)
    val BUCKET_NAME = config.getString("BUCKET_NAME")
    val FILE_PATH = config.getString("FILE_PATH")
    val FILE_NAME = config.getString("FILE_NAME")
    val logFuture = Future {
      LogMsgSimulator(init(RandomStringGenerator((Parameters.minStringLength, Parameters.maxStringLength), Parameters.randomSeed)), Parameters.maxCount)
    }
    Try(Await.result(logFuture, Parameters.runDurationInMinutes)) match {
      case Success(value) => logger.info(s"Log data generation has completed after generating ${Parameters.maxCount} records.")
      case Failure(exception) => logger.info(s"Log data generation has completed within the allocated time, ${Parameters.runDurationInMinutes}")
    }
    try {
      val amazonS3Client = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_2).build
      // upload file
      logger.info("Preparing to upload the file...")
      val file = new File(FILE_PATH)
      amazonS3Client.putObject(BUCKET_NAME, FILE_NAME, file)
      logger.info("File uploaded successfully")
    }
    catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
    }

class GenerateLogData

