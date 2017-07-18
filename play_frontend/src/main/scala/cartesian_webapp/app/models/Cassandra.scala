package models

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSetFuture
import com.datastax.driver.core.Session
import scala.collection.JavaConversions._
import play.api.Logger
import com.datastax.driver.core.Metadata

/**
 * Simple cassandra client, following the datastax documentation
 * (http://www.datastax.com/documentation/developer/java-driver/2.0/java-driver/quick_start/qsSimpleClientCreate_t.html).
 */
class SimpleClient(node: String) {

	private val cluster = Cluster.builder().addContactPoint(node).build()
	log(cluster.getMetadata())
	val session = cluster.connect()

  private def log(metadata: Metadata): Unit = {
    Logger.info(s"Connected to cluster: ${metadata.getClusterName}")
    for (host <- metadata.getAllHosts()) {
      Logger.info(s"Datatacenter: ${host.getDatacenter()}; Host: ${host.getAddress()}; Rack: ${host.getRack()}")
    }
  }

  def close() {
    session.close
    cluster.close
  }

  def sample_query() {
    val query = "SELECT * FROM plume.location_lat_lon LIMIT 5;"
    val coordinatesTable = session.execute(query)
    coordinatesTable.all()
  }

  object Cassandra extends App {
  val client = new SimpleClient("104.196.98.41")
  client.sample_query
  client.close
}
}