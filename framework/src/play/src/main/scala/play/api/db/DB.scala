package play.api.db

import play.api._
import play.api.libs._

import play.core._

import java.sql._
import javax.sql._
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.mchange.v2.c3p0.ConnectionCustomizer


/**
 * The Play Database API manages several connection pools.
 *
 * @param datasources the managed data sources
 */
trait DBApi {

  val datasources: List[Tuple2[DataSource, String]]
  
  /**
   * shut down pool for given datasource
   * @param ds 
   */
  def shutdownPool(ds: DataSource)
  /**
   * Retrieves a JDBC connection, with auto-commit set to `true`.
   *
   * Don’t forget to release the connection at some point by calling close().
   *
   * @param name the data source name
   * @return a JDBC connection
   * @throws an error if the required data source is not registered
   */
  def getDataSource(name: String): DataSource 

/**
   * Retrieves the JDBC connection URL for a particular data source.
   *
   * @param name the data source name
   * @return The JDBC URL connection string, i.e. `jdbc:…`
   * @throws an error if the required data source is not registered
   */
  def getDataSourceURL(name: String): String = {
    val ds = getDataSource(name)
    ds.getConnection.getMetaData.getURL
  }
   /**
   * Retrieves a JDBC connection.
   *
   * Don’t forget to release the connection at some point by calling close().
   *
   * @param name the data source name
   * @param autocommit when `true`, sets this connection to auto-commit
   * @return a JDBC connection
   * @throws an error if the required data source is not registered
   */
  def getConnection(name: String, autocommit: Boolean = true): Connection = {
    val connection = getDataSource(name).getConnection
    connection.setAutoCommit(autocommit)
    connection
  }
  /**
   * Execute a block of code, providing a JDBC connection. The connection is
   * automatically released.
   *
   * @param name The datasource name.
   * @param block Code block to execute.
   */
  def withConnection[A](name: String)(block: Connection => A): A = {
    val connection = getConnection(name)
    try {
      block(connection)
    } finally {
      connection.close()
    }
  }

  /**
   * Execute a block of code, in the scope of a JDBC transaction.
   * The connection is automatically released.
   * The transaction is automatically committed, unless an exception occurs.
   *
   * @param name The datasource name.
   * @param block Code block to execute.
   */
  def withTransaction[A](name: String)(block: Connection => A): A = {
    val connection = getConnection(name)
    try {
      connection.setAutoCommit(false)
      val r = block(connection)
      connection.commit()
      r
    } catch {
      case e => connection.rollback(); throw e
    } finally {
      connection.close()
    }
  }

}



/**
 * Provides a high-level API for getting JDBC connections.
 *
 * For example:
 * {{{
 * val conn = DB.getConnection("customers")
 * }}}
 */
object DB {

  /** The exception we are throwing. */
  private def error = throw new Exception("DB plugin is not registered.")

  /**
   * Retrieves a JDBC connection.
   *
   * @param name data source name
   * @param autocommit when `true`, sets this connection to auto-commit
   * @return a JDBC connection
   * @throws an error if the required data source is not registered
   */
  def getConnection(name: String = "default", autocommit: Boolean = true)(implicit app: Application): Connection = app.plugin[DBPlugin].map(_.api.getConnection(name, autocommit)).getOrElse(error)

  /**
   * Retrieves a JDBC connection (autocommit is set to true).
   *
   * @param name data source name
   * @return a JDBC connection
   * @throws an error if the required data source is not registered
   */
  def getDataSource(name: String = "default")(implicit app: Application): DataSource = app.plugin[DBPlugin].map(_.api.getDataSource(name)).getOrElse(error)

  /**
   * Execute a block of code, providing a JDBC connection. The connection is
   * automatically released.
   *
   * @param name The datasource name.
   * @param block Code block to execute.
   */
  def withConnection[A](name: String)(block: Connection => A)(implicit app: Application): A = {
    app.plugin[DBPlugin].map(_.api.withConnection(name)(block)).getOrElse(error)
  }

  /**
   * Execute a block of code, providing a JDBC connection. The connection is
   * automatically released.
   *
   * @param block Code block to execute.
   */
  def withConnection[A](block: Connection => A)(implicit app: Application): A = {
    app.plugin[DBPlugin].map(_.api.withConnection("default")(block)).getOrElse(error)
  }

  /**
   * Execute a block of code, in the scope of a JDBC transaction.
   * The connection is automatically released.
   * The transaction is automatically committed, unless an exception occurs.
   *
   * @param name The datasource name.
   * @param block Code block to execute.
   */
  def withTransaction[A](name: String = "default")(block: Connection => A)(implicit app: Application): A = {
    app.plugin[DBPlugin].map(_.api.withTransaction(name)(block)).getOrElse(error)
  }

  /**
   * Execute a block of code, in the scope of a JDBC transaction.
   * The connection is automatically released.
   * The transaction is automatically committed, unless an exception occurs.
   *
   * @param block Code block to execute.
   */
  def withTransaction[A](block: Connection => A)(implicit app: Application): A = {
    app.plugin[DBPlugin].map(_.api.withTransaction("default")(block)).getOrElse(error)
  }

}

/**
 *
 * Default customizer for all C3P0 connections.
 * autocommit and isolation are expected to be set globally
 * since we have no way of knowing the database name 
 *(this class is created via reflection by the pool).
 *
 * different isolation/autocommit behaviour can be provided for any database
 * by defining a db.<dbname>.pool.customizer key in application.conf
 */
class C3P0ConnectionCustomizer extends ConnectionCustomizer {

  private val conf = play.api.Configuration.load()
  
  private val autocommit = conf.getBoolean("db.autocommit").getOrElse(true)

  private val isolation = conf.getString("db.isolation").getOrElse("READ_COMMITTED") match {
      case "NONE" => Connection.TRANSACTION_NONE
      case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
      case "READ_UNCOMMITTED " => Connection.TRANSACTION_READ_UNCOMMITTED
      case "REPEATABLE_READ " => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
      case unknown => throw conf.reportError("isolation", "Unknown isolation level [" + unknown + "]")
  }
  def onAcquire(c: Connection, parentDataSourceIdentityToken: String) {
      c.setTransactionIsolation(isolation)
      c.setAutoCommit(autocommit)
  }   

   def onDestroy(c: Connection, parentDataSourceIdentityToken: String) {}
   def onCheckOut(c: Connection, parentDataSourceIdentityToken: String) {}
   def onCheckIn(c: Connection, parentDataSourceIdentityToken: String) {}

}

/**
 * C3P0 implementation of the DBApi
 * @param c configuration for the given db
 * @param dbName, a datasource will be created only for the given dbName
 * @param classloader 
 */
class C3P0Api(c: Configuration, dbName: Option[String], classloader: ClassLoader = ClassLoader.getSystemClassLoader) extends DBApi {


   /** The exception we are throwing. */
  private def error(m: String="") = throw new Exception("invalid DB configuration "+m)


  private def register(driver: String) {
      try {
        DriverManager.registerDriver(new play.utils.ProxyDriver(Class.forName(driver, true, classloader).newInstance.asInstanceOf[Driver]))
      } catch {
        case e => throw c.reportError("driver", "Driver not found: [" + driver + "]", Some(e))
      }
    }

  private def createDataSource(dbName: String, url: String, driver:String, c: Configuration): DataSource = {
    var ds = new ComboPooledDataSource()
    ds.setDriverClass(driver)
    ds.setJdbcUrl(url)
    c.getString("pool.user").map(e => ds.setUser(e)).getOrElse(Unit)
    c.getString("pool.password").map(e => ds.setPassword(e)).getOrElse(Unit)
    ds.setAcquireRetryAttempts(c.getInt("pool.retry.attempt").getOrElse(10))
    ds.setCheckoutTimeout(c.getMilliseconds("pool.timeout").map(_.toInt).getOrElse(5000))
    ds.setBreakAfterAcquireFailure(false)
    ds.setMaxPoolSize(c.getInt("pool.maxSize").getOrElse(30))
    ds.setMinPoolSize(c.getInt("pool.minSize").getOrElse(1))
    ds.setMaxIdleTimeExcessConnections(c.getInt("pool.maxIdleTimeExcessConnections").getOrElse(0))
    ds.setIdleConnectionTestPeriod(10)
    ds.setTestConnectionOnCheckin(true)
    // Bind JNDI
    c.getString("jndiName").map { name =>
      JNDI.initialContext.rebind(name, ds)
      Logger("play").info("datasource [" + url + "] bound to JNDI as " + name)
    }   
    // set customizer
    ds.setConnectionCustomizerClassName(c.getString("pool.customizer").getOrElse(classOf[C3P0ConnectionCustomizer].getName))  
    ds               
  }
   
  private val dbNames = dbName.map(n=> Set(n)).getOrElse(c.subKeys.filter{
    !(k == "isolation" || k == "autocommit")
  })   
  //register either a specific connection or all of them 
  val datasources: List[Tuple2[DataSource, String]] = dbNames.map { dbName =>
      val url = c.getString(dbName+".url").getOrElse(error("- could not determine url for "+dbName+".url")) 
      val driver = c.getString(dbName+".driver").getOrElse(error("- could not determine driver for "+dbName+".driver"))    
      val extraConfig = c.getConfig(dbName).getOrElse(error("- could not find extra configs")) 
      register(driver)
      createDataSource(dbName, url, driver, extraConfig) -> dbName
    }.toList
  
  def shutdownPool(ds: DataSource) = {
    if (ds.isInstanceOf[ComboPooledDataSource]) {
      ds.asInstanceOf[ComboPooledDataSource].hardReset()
    } else {
      throw new Exception("could not recognize DataSource, therefore unable to shutdown this pool")
    }
  }

  /**
   * Retrieves a JDBC connection, with auto-commit set to `true`.
   *
   * Don’t forget to release the connection at some point by calling close().
   *
   * @param name the data source name
   * @return a JDBC connection
   * @throws an error if the required data source is not registered
   */
  def getDataSource(name: String): DataSource = {
    datasources.filter(_._2 == name).headOption.map(e => e._1).getOrElse(error(" - could not find datasource for "+name))
  }

}

/**
 * Generic DBPlugin interface
 */
trait DBPlugin extends Plugin {
  def api: DBApi
}

/**
 * a DBPlugin implementation that provides a DBApi
 *
 * @param app the application that is registering the plugin
 */
class C3P0Plugin(app: Application) extends DBPlugin {

    /** The exception we are throwing. */
  private def error = throw new Exception("db keys are missing from application.conf")

  //*configuration for db-s
  private val dbConfig = app.configuration.getConfig("db")
  
  //should be accessed in onStart first
  private lazy val dbApi: DBApi = new C3P0Api(dbConfig.getOrElse(error), None, app.classloader)
  
  /**
   * plugin is disabled if either configuration is missing or the plugin is explicitly disabled
   */
  private lazy val isDisabled = {
    app.configuration.getString("dbplugin").filter(_ == "disabled").isDefined || dbConfig.isDefined == false
  }
 /**
   * Is this plugin enabled.
   *
   * {{{
   * dbplugin=disabled
   * }}}
   */
  override def enabled = isDisabled == false

  /**
   * Retrieves the underlying `DBApi` managing the data sources.
   */
  def api: DBApi = dbApi

  /**
   * Reads the configuration and connects to every data source.
   */
  override def onStart() {
    //try to connect to each, this should be the first access to dbApi
    dbApi.datasources.map { ds =>
      try {
        ds._1.getConnection.close()
        app.mode match {
          case Mode.Test =>
          case mode => Logger("play").info("database [" + ds._2 + "] connected at " + ds._1.getConnection.getMetaData.getURL )
        }
      } catch {
        case e => {
          throw dbConfig.getOrElse(error).reportError("url", "Cannot connect to database at [" + ds._1.getConnection.getMetaData.getURL  + "]", Some(e.getCause))
        }
      }
    }
  }

  /**
   * Closes all data sources.
   */
  override def onStop() {
    dbApi.datasources.foreach {
      case (ds,_) => try {
        dbApi.shutdownPool(ds)
      } catch { case _ => }
    }
    dbConfig.getOrElse(error).getString("driver").map { driver =>
      //wait for the pool's threads to shut down
      Thread.sleep(100)
      val drivers = DriverManager.getDrivers()
      while (drivers.hasMoreElements) {
        val driver = drivers.nextElement
        DriverManager.deregisterDriver(driver)
      }
    }
  }

}
