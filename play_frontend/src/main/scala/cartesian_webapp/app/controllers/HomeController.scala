package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import com.datastax.driver.core.Cluster
import models.SimpleClient

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject() extends Controller {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {
    implicit val session = new Cluster
    .Builder()
      .addContactPoints("104.196.29.34")
      .withPort(9042)
      .build()
      .connect()
    val query = s"SELECT * FROM plume.location_lat_lon LIMIT 5;"
    val coordinatesTable = session.execute(query)
    val c = coordinatesTable.all().toString
    Ok(views.html.test(c))
  }

}