package TaxiTrip

import com.esri.core.geometry.{Geometry, GeometryEngine}
import org.json4s.JObject
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read
// https://github.com/haghard/streams-recipes/blob/master/nyc-borough-boundaries-polygon.geojson


case class FeatureCollection(features:List[Feature])

case class Feature(
                    properties: Map[String, String],
                    geometry: JObject
                  ) {
  def getGeometry():Geometry = {
    import org.json4s.jackson.JsonMethods._
    val mapGeo = GeometryEngine.geoJsonToGeometry(compact(render(geometry)), 0, Geometry.Type.Unknown)
    mapGeo.getGeometry
  }
}

object FeatureExtraction{
  def parseJson(json:String): FeatureCollection = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val featureCollection = read[FeatureCollection](json)
    featureCollection
  }
}


