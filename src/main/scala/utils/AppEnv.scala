package utils

import io.github.cdimascio.dotenv.Dotenv
import scala.collection.JavaConverters._

/**
 * A Singleton Object for Managing Environment Variables
 */
object AppEnv {
  // Load environment variables from the .env file
  private val dotenv: Dotenv = Dotenv.configure().ignoreIfMissing().load()

  // Store variables in a Scala immutable Map for thread-safe global access
  private val envVars: Map[String, String] = dotenv.entries().asScala.map(entry => entry.getKey -> entry.getValue).toMap

  /**
   * Get the value of an environment variable
   * @param key The name of the environment variable
   * @return An Option containing the variable's value, if found
   */
  def get(key: String): Option[String] = envVars.get(key)

  /**
   * Get the value of an environment variable with a default
   * @param key The name of the environment variable
   * @param default The default value if the variable is not found
   * @return The value of the environment variable, or the default
   */
  def getOrElse(key: String, default: String): String = envVars.getOrElse(key, default)
}