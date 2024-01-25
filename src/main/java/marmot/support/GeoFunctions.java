package marmot.support;

import java.util.List;
import java.util.Map;

import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.Geometries;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.util.Base64;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import marmot.geo.GeoClientUtils;
import marmot.geo.GeoJsonReader;
import marmot.type.MapTile;
import utils.script.MVELFunction;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoFunctions {
	private static final Logger s_logger = LoggerFactory.getLogger(GeoFunctions.class);
	
	/*
	 ********************************************************************************
	 * Spatial Relationship
	 ********************************************************************************
	 ********************************************************************************/

	@MVELFunction(name="ST_IsEmpty")
	public static boolean ST_IsEmpty(Object obj) {
		Geometry geom = asGeometry(obj);
        try {
			boolean flag = (geom == null || geom.isEmpty() || geom.getNumGeometries() == 0);
			return flag;
		}
		catch ( Exception e ) {
			return true;
		}
	}

	@MVELFunction(name="ST_Equals")
	public static boolean ST_Equals(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.equals(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Disjoint")
	public static boolean ST_Disjoint(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.disjoint(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Intersects")
	public static boolean ST_Intersects(Object obj1, Object obj2) {
		Geometry geom1 = (obj1 instanceof Envelope)
						? GeoClientUtils.toPolygon((Envelope)obj1) : asGeometry(obj1);
		Geometry geom2 = (obj2 instanceof Envelope)
						? GeoClientUtils.toPolygon((Envelope)obj2) : asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.intersects(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Touches")
	public static boolean ST_Touches(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.touches(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Crosses")
	public static boolean ST_Crosses(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.crosses(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Within")
	public static boolean ST_Within(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.within(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_DWithin")
	public static boolean ST_DWithin(Object obj1, Object obj2, Object distance) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			double dist = DataUtils.asDouble(distance);
			return geom1.isWithinDistance(geom2, dist);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Contains")
	public static boolean ST_Contains(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.contains(geom2);
		}
		else {
			return false;
		}
	}

	@MVELFunction(name="ST_Overlaps")
	public static boolean ST_Overlaps(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		Geometry geom2 = asGeometry(obj2);
		if ( geom1 != null && geom2 != null ) {
			return geom1.overlaps(geom2);
		}
		else {
			return false;
		}
	}
	
	/********************************************************************************
	 ********************************************************************************
	 * Spatial Operators
	 ********************************************************************************
	 ********************************************************************************/

	/**
	 * 주어진 두 공간 객체의 교집합 공간 객체를 구한다.
	 * 
	 * @param obj1	공간 객체.
	 * @param obj2	공간 객체.
	 * @return	교집합 공간 객체.
	 */
	@MVELFunction(name="ST_Intersection")
	public static Geometry ST_Intersection(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		if ( geom1 == null ) {
			return null;
		}
		Geometry geom2 = asGeometry(obj2);
		if ( geom2 == null ) {
			return null;
		}
		return geom1.intersection(geom2);
	}

	@MVELFunction(name="ST_Union")
	public static Geometry ST_Union(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		if ( geom1 == null ) {
			return null;
		}
		Geometry geom2 = asGeometry(obj2);
		if ( geom2 == null ) {
			return null;
		}
		return geom1.union(geom2);
	}

	@MVELFunction(name="ST_Difference")
	public static Geometry ST_Difference(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		if ( geom1 == null ) {
			return null;
		}
		Geometry geom2 = asGeometry(obj2);
		if ( geom2 == null ) {
			return null;
		}
		return geom1.difference(geom2);
	}

	@MVELFunction(name="ST_SymDifference")
	public static Geometry ST_SymDifference(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		if ( geom1 == null ) {
			return null;
		}
		Geometry geom2 = asGeometry(obj2);
		if ( geom2 == null ) {
			return null;
		}
		return geom1.symDifference(geom2);
	}

	@MVELFunction(name="ST_Buffer")
	public static Geometry ST_Buffer(Object obj, Object distObj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		
		double dist = DataUtils.asDouble(distObj);
		return geom.buffer(dist);
	}

	@MVELFunction(name="ST_ConvexHull")
	public static Geometry ST_ConvexHull(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom != null ) {
			return geom.convexHull();
		}
		else {
			return null;
		}
	}

	@MVELFunction(name="ST_Centroid")
	public static Geometry ST_Centroid(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		
		return geom.getCentroid();
	}

	@MVELFunction(name="ST_MakePolygon")
	public static Polygon ST_MakePolygon(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null || geom.isEmpty() || !(geom instanceof MultiPoint) ) {
			return ST_EmptyPolygon();
		}
		
		return GeoUtils.makePolygon(((MultiPoint)obj).getCoordinates());
	}

//	@MVELFunction(name="ST_ConcaveHull")
//	public static Polygon ST_ConcaveHull(Object obj, Object othreshold) {
//		Geometry geom = asGeometry(obj);
//		if ( geom == null || geom.isEmpty() || !(geom instanceof MultiPoint) ) {
//			return ST_EmptyPolygon();
//		}
//		double threshold = DataUtils.asDouble(othreshold);
//		
//		return (Polygon)GeoUtils.makeConcaveHull((MultiPoint)obj, threshold);
//	}

	@MVELFunction(name="ST_MakePolygon2")
	public static Polygon ST_MakePolygon2(Object obj, Object center) {
		Geometry geom = asGeometry(obj);
		if ( geom == null || geom.isEmpty() || !(geom instanceof MultiPoint) ) {
			return ST_EmptyPolygon();
		}

		Geometry centroid = asGeometry(center);
		if ( centroid == null || centroid.isEmpty() || !(centroid instanceof Point) ) {
			return ST_EmptyPolygon();
		}
		
		return GeoUtils.makePolygon(((MultiPoint)obj).getCoordinates(),
									centroid.getCoordinate());
	}
	
	//********************************************************************************
	//********************************************************************************
	// Proximity & Measurement Operators
	//********************************************************************************
	//********************************************************************************

	@MVELFunction(name="ST_Distance")
	public static double ST_Distance(Object obj1, Object obj2) {
		Geometry geom1 = asGeometry(obj1);
		if ( geom1 == null ) {
			return -1;
		}
		Geometry geom2 = asGeometry(obj2);
		if ( geom2 == null ) {
			return -1;
		}
		return geom1.distance(geom2);
	}

	@MVELFunction(name="ST_Area")
	public static double ST_Area(Object obj) {
		Geometry geom = asGeometry(obj);
		return (geom != null) ? geom.getArea() : -1;
	}

	@MVELFunction(name="ST_Length")
	public static double ST_Length(Object obj) {
		Geometry geom = asGeometry(obj);
		return (geom != null) ? geom.getLength() : -1;
	}

	@MVELFunction(name="ST_ComponentCount")
	public static double ST_ComponentCount(Object obj) {
		Geometry geom = asGeometry(obj);
		return (geom != null) ? geom.getNumGeometries() : -1;
	}
	
	//********************************************************************************
	//********************************************************************************
	// Geometry Input/Output
	//********************************************************************************
	//********************************************************************************

	@MVELFunction(name="ST_AsEnvelope")
	public static Envelope toEnvelope(Object geom) {
		if ( geom == null ) {
			return null;
		}
		else if ( geom instanceof Geometry ) {
			return ((Geometry)geom).getEnvelopeInternal();
		}
		else {
			s_logger.error("ST_AsEnvelope should take Geometry type: " + geom.getClass());
			throw new IllegalArgumentException();
		}
	}

	@MVELFunction(name="ST_GeomFromEnvelope")
	public static Geometry ST_GeomFromEnvelope(Object obj) {
		if ( obj == null ) {
			return null;
		}
		else if ( obj instanceof Envelope ) {
			return GeoClientUtils.toPolygon((Envelope)obj);
		}
		else {
			s_logger.error("ST_GeomFromEnvelope should take Envelope type: " + obj.getClass());
			throw new IllegalArgumentException();
		}
	}
	
	@MVELFunction(name="ST_GeomFromText")
	public static Geometry ST_GeomFromText(Object wktStr) throws ParseException {
		if ( wktStr == null ) {
			return null;
		}
		else if ( wktStr instanceof String ) {
			return GeoClientUtils.fromWKT((String)wktStr);
		}
		else {
			s_logger.error("ST_GeomFromText should take string type: " + wktStr.getClass());
			throw new IllegalArgumentException();
		}
	}
	
	@MVELFunction(name="ST_AsText")
	public static String ST_AsText(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		
		return GeoClientUtils.toWKT(geom);
	}

	@MVELFunction(name="ST_GeomFromWKB")
	public static Geometry ST_GeomFromWKB(Object bytes) throws ParseException {
		if ( bytes == null ) {
			return null;
		}
		else if ( bytes instanceof byte[] ) {
			return GeoClientUtils.fromWKB((byte[])bytes);
		}
		else {
			s_logger.error("ST_GeomFromWKB should take binary type: " + bytes.getClass());
			throw new IllegalArgumentException();
		}
	}
	
	@MVELFunction(name="ST_AsBinary")
	public static byte[] ST_AsBinary(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		
		return GeoClientUtils.toWKB(geom);
	}
	
	@MVELFunction(name="ST_MapTileFromQuadKey")
	public static MapTile ST_MapTileFromQuadKey(Object quadKey) throws ParseException {
		if ( quadKey == null ) {
			return null;
		}
		else if ( quadKey instanceof String ) {
			return MapTile.fromQuadKey((String)quadKey);
		}
		else {
			s_logger.error("ST_MapTileFromQuadKey should take string type: " + quadKey.getClass());
			throw new IllegalArgumentException();
		}
	}
	
	@MVELFunction(name="ST_AsHexString")
	public static String ST_AsHexString(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return "";
		}
		
		return WKBWriter.toHex(ST_AsBinary(obj));
	}
	
	@MVELFunction(name="ST_GeomFromHex")
	public static Geometry ST_GeomFromHex(Object hex) throws ParseException {
		if ( hex == null ) {
			return null;
		}
		else if ( hex instanceof String ) {
			return GeoClientUtils.fromWKB(WKBReader.hexToBytes((String)hex));
		}
		else {
			s_logger.error("ST_GeomFromHex should take string type: " + hex.getClass());
			throw new IllegalArgumentException();
		}
	}
	
	@MVELFunction(name="ST_AsBase64")
	public static String ST_AsBase64(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return "";
		}
		
		return Base64.encodeBytes(GeoClientUtils.toWKB(geom));
	}
	
	@MVELFunction(name="ST_GeomFromBase64")
	public static Geometry ST_GeomFromBase64(Object obj) throws ParseException {
		if ( obj == null ) {
			return null;
		}
		else if ( obj instanceof String ) {
			String str = (String)obj;
			if ( str.length() == 0 ) {
				return null;
			}
			
			byte[] wkb = Base64.decode(str);
			return GeoClientUtils.fromWKB(wkb);
		}
		else {
			s_logger.error("ST_GeomFromBase64 should take string type: " + obj.getClass());
			throw new IllegalArgumentException();
		}
	}

	@MVELFunction(name="ST_AsGeoJSON")
	public static String ST_AsGeoJSON(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( geom == null ) {
			return null;
		}
		
		return new GeometryJSON().toString(geom);
	}

	@SuppressWarnings("unchecked")
	@MVELFunction(name="ST_GeomFromGeoJSON")
	public static Geometry ST_GeomFromGeoJSON(Object json) {
		try {
			if ( json == null ) {
				return null;
			}
			
			if ( json instanceof String ) {
				return GeoJsonReader.read(JsonParser.parse((String)json));
			}
			else if ( json instanceof Map ) {
				return GeoJsonReader.read((Map<String,Object>)json);
			}
			else {
				s_logger.error(String.format("fails to parse GeoJSON: '%s'", json));
				return null;
			}
		}
		catch ( Exception e ) {
			s_logger.error(String.format("fails to parse GeoJSON: '%s'", json), e);
			return null;
		}
	}

	@MVELFunction(name="ST_Point")
	public static Point ST_Point(Object x, Object y) {
		if ( x == null || y == null ) {
			return ST_EmptyPoint();
		}
		
		try {
			Double xpos = DataUtils.asDouble(x);
			Double ypos = DataUtils.asDouble(y);
			
			return GeoClientUtils.toPoint(xpos, ypos);
		}
		catch ( Exception e ) {
			s_logger.error(String.format("failed toPoint(%s,%s)", x, y), e);
			return ST_EmptyPoint();
		}
	}

	@MVELFunction(name="ST_Validate")
	public static Geometry ST_Validate(Object obj) {
		Geometry geom = asGeometry(obj);
		if ( !geom.isValid() ) {
			Geometries type = Geometries.get(geom);
			
			geom = GeoClientUtils.makeValid(geom);
			if ( !geom.isEmpty() && geom.isValid() ) {
				geom = GeoClientUtils.cast(geom, type);
			}
			else {
				geom = GeoClientUtils.emptyGeometry(Geometries.get(geom));
			}
		}
		
		return geom;
	}
	
	//********************************************************************************
	//********************************************************************************
	// Geometry Accessor
	//********************************************************************************
	//********************************************************************************

	@MVELFunction(name="ST_X")
	public static double getX(Object pt) {
		if ( pt == null ) {
			return 0;
		}
		else if ( pt instanceof Point ) {
			return ((Point)pt).getX();
		}
		else if ( pt instanceof Coordinate ) {
			return ((Coordinate)pt).x;
		}
		else {
			throw new IllegalArgumentException("cannt get_x from object=" + pt);
		}
	}

	@MVELFunction(name="ST_Y")
	public static double getY(Object pt) {
		if ( pt == null ) {
			return 0;
		}
		else if ( pt instanceof Point ) {
			return ((Point)pt).getY();
		}
		else if ( pt instanceof Coordinate ) {
			return ((Coordinate)pt).y;
		}
		else {
			throw new IllegalArgumentException("cannt get_y from object=" + pt);
		}
	}
	
	public static String toEPSG(CoordinateReferenceSystem crs) {
		return Iterables.getFirst(crs.getIdentifiers(), null).toString();
	}

	public static Envelope asLonLatBounds(MapTile tile) {
		return tile.getBounds();
	}

	public static MapTile toTile(int tileX, int tileY, int zoom) {
		return new MapTile(zoom, tileX, tileY);
	}

//	@MVELFunction(name="ST_REMOVE_HOLES")
//	public static MultiPolygon removeSmallHoles(Geometry geom, double threshold) {
//		switch ( Geometries.get(geom) ) {
//			case POLYGON:
//				return GeoFunctions.GEOM_FACT.createMultiPolygon(
//									new Polygon[]{GeoUtils.removeSmallHoles((Polygon)geom, threshold)});
//			case MULTIPOLYGON:
//			case GEOMETRYCOLLECTION:
//				Polygon[] polys = GeoUtils.flattenToPolygonList(geom).stream()
//											.map(p -> GeoUtils.removeSmallHoles(p, threshold))
//											.toArray(sz -> new Polygon[sz]);
//				return GeoFunctions.GEOM_FACT.createMultiPolygon(polys);
//			default:
//				throw new IllegalArgumentException("cannot remove holes from non-polygon geometry: " + geom);
//		}
//	}

	@MVELFunction(name="ST_EmptyPoint")
	public static Point ST_EmptyPoint() {
		return GeoClientUtils.EMPTY_POINT;
	}

	@MVELFunction(name="ST_EmptyMultiPoint")
	public static MultiPoint ST_EmptyMultiPoint() {
		return GeoClientUtils.EMPTY_MULTIPOINT;
	}

	@MVELFunction(name="ST_EmptyLineString")
	public static LineString ST_EmptyLineString() {
		return GeoClientUtils.EMPTY_LINESTRING;
	}

	@MVELFunction(name="ST_EmptyMultiLineString")
	public static MultiLineString ST_EmptyMultiLineString() {
		return GeoClientUtils.EMPTY_MULTILINESTRING;
	}

	@MVELFunction(name="ST_EmptyPolygon")
	public static Polygon ST_EmptyPolygon() {
		return GeoClientUtils.EMPTY_POLYGON;
	}

	@MVELFunction(name="ST_EmptyMultiPolygon")
	public static MultiPolygon ST_EmptyMultiPolygon() {
		return GeoClientUtils.EMPTY_MULTIPOLYGON;
	}

	@MVELFunction(name="ST_EmptyGeomCollection")
	public static GeometryCollection ST_EmptyGeomCollection() {
		return GeoClientUtils.EMPTY_GEOM_COLLECTION;
	}

	@MVELFunction(name="ST_EmptyGeometry")
	public static Geometry ST_EmptyGeometry() {
		return GeoClientUtils.EMPTY_GEOMETRY;
	}

	@MVELFunction(name="ST_AddGeometry")
	public static Object ST_AddGeometry(Object ocoll, Object ocomp) {
		Geometry tail = asGeometry(ocomp);
		if ( tail == null || tail.isEmpty() ) {
			return ocoll;
		}
		
		Geometry coll = asGeometry(ocoll);
		if ( coll == null || !(coll instanceof GeometryCollection) ) {
			return ocoll;
		}
		
		FStream<Geometry> comps = GeoUtils.streamSubComponents(coll).concatWith(tail);
		switch ( Geometries.get(coll) ) {
			case MULTIPOINT:
				return GeoClientUtils.toMultiPoint(comps.castSafely(Point.class)
														.toArray(Point.class));
			case MULTILINESTRING:
				return GeoClientUtils.toMultiLineString(comps.castSafely(LineString.class)
															.toArray(LineString.class));
			case MULTIPOLYGON:
				return GeoClientUtils.toMultiPolygon(comps.castSafely(Polygon.class)
															.toArray(Polygon.class));
			case GEOMETRYCOLLECTION:
				return GeoClientUtils.GEOM_FACT.createGeometryCollection(
														comps.toArray(Geometry.class));
			default:
				throw new AssertionError();
		}
	}

	@MVELFunction(name="ST_ClosestPointOnLine")
	public static Point findClosestPointOnLineString(Point obj1, Object obj2) {
		Preconditions.checkArgument(obj1 instanceof Point,
									"obj1 should be a Point, but " + obj1);
		
		LineString line;
		if ( obj2 instanceof LineString ) {
			line = (LineString)obj2;
		}
		else if ( obj2 instanceof MultiLineString ) {
			List<LineString> lines = GeoClientUtils.fstream((MultiLineString)obj2)
													.cast(LineString.class)
													.toList();
			if ( lines.size() == 1 ) {
				line = lines.get(0);
			}
			else {
				throw new IllegalArgumentException("obj2 should have just one LineString, but "
													+ lines.size() + " lines");
			}
		}
		else {
			throw new IllegalArgumentException("obj2 should be a LineString, but " + obj2);
		}

		Point pt = (Point)obj1;
		return GeoUtils.findClosestPointOnLine(pt, line)._1;
	}

	public static Geometry asGeometry(Object obj) {
		if ( obj == null ) {
			return null;
		}
		else if ( obj instanceof Geometry ) {
			return (Geometry)obj;
		}
		else if ( obj instanceof Envelope ) {
			return GeoClientUtils.toPolygon((Envelope)obj);
		}
		else {
			throw new IllegalArgumentException("Not Geometry: obj=" + obj);
		}
	}
}
