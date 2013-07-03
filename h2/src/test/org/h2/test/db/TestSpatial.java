/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.h2.test.TestBase;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import org.h2.value.ValueGeometry;

/**
 * Spatial datatype and index tests.
 */
public class TestSpatial extends TestBase {

    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("spatial");
        testSpatialValues();
        testMemorySpatialIndex();
        deleteDb("spatial");
    }

    private void testSpatialValues() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection("spatial");
        Statement stat = conn.createStatement();
        
        stat.execute("create memory table test(id int primary key, poly geometry)");
        stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("POLYGON ((1 1, 1 2, 2 2, 1 1))", rs.getString(2));
        GeometryFactory f = new GeometryFactory();
        Polygon poly = f.createPolygon(new Coordinate[] { new Coordinate(1,1), new Coordinate(1,2), new Coordinate(2,2), new Coordinate(1, 1) });
        assertTrue(poly.equals(rs.getObject(2)));
       
        rs = stat.executeQuery("select * from test where poly = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        stat.executeQuery("select * from test where poly > 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        stat.executeQuery("select * from test where poly < 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        
        stat.execute("drop table test");
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Generate a random linestring under the given bounding box
     * @param minX Bounding box min x
     * @param maxX Bounding box max x
     * @param minY Bounding box min y
     * @param maxY Bounding box max y
     * @param maxLength LineString maximum length
     * @return A segment within this bounding box
     */
    public static Geometry getRandomGeometry(Random geometryRand,double minX,double maxX,double minY, double maxY, double maxLength) {
        GeometryFactory factory = new GeometryFactory();
        // Create the start point
        Coordinate start = new Coordinate(geometryRand.nextDouble()*(maxX-minX)+minX,
                geometryRand.nextDouble()*(maxY-minY)+minY);
        // Compute an angle
        double angle = geometryRand.nextDouble() * Math.PI * 2;
        // Compute length
        double length = geometryRand.nextDouble() * maxLength;
        // Compute end point
        Coordinate end = new Coordinate(start.x + Math.cos(angle) * length, start.y + Math.sin(angle) * length);
        return factory.createLineString(new Coordinate[]{start,end});
    }

   private void testRandom(Connection conn, long seed,long size) throws SQLException {
       Statement stat = conn.createStatement();
       stat.execute("drop table if exists test");
       Random geometryRand = new Random(seed);
       // Generate a set of geometry
       // It is marked as random, but it generate always the same geometry set, given the same seed
       stat.execute("create memory table test(id long primary key auto_increment, poly geometry)");
       // Create segment generation bounding box
       Envelope bbox = ValueGeometry.get("POLYGON ((301804.1049793153 2251719.1222191923," +
               " 301804.1049793153 2254747.2888244865, 304646.87362918374 2254747.2888244865," +
               " 304646.87362918374 2251719.1222191923, 301804.1049793153 2251719.1222191923))")
               .getGeometry().getEnvelopeInternal();
       // Create overlap test bounding box
       String testBBoxString = "POLYGON ((302215.44416332216 2252748, 302215.44416332216 2253851.781225762," +
               " 303582.85796541866 2253851.781225762, 303582.85796541866 2252748.526908161," +
               " 302215.44416332216 2252748))";
       Envelope testBBox = ValueGeometry.get(testBBoxString).getGeometry().getEnvelopeInternal();

       PreparedStatement ps = conn.prepareStatement("insert into test(poly) values (?)");
       long overlapCount = 0;
       Set<Integer> overlaps = new HashSet<Integer>(680);
       for(int i=1;i<=size;i++) {
           Geometry geometry = getRandomGeometry(geometryRand,bbox.getMinX(),bbox.getMaxX(),bbox.getMinY(),bbox.getMaxY(),200);
           ps.setObject(1,geometry);
           ps.execute();
           ResultSet keys = ps.getGeneratedKeys();
           keys.next();
           if(geometry.getEnvelopeInternal().intersects(testBBox)) {
               overlapCount++;
               overlaps.add(keys.getInt(1));
           }
       }
       ps.close();
       // Create index
       stat.execute("create spatial index idx_test_poly on test(poly)");
       // Must find the same overlap count with index
       ps = conn.prepareStatement("select id from test where poly && ?::Geometry");
       ps.setString(1,testBBoxString);
       ResultSet rs = ps.executeQuery();
       long found = 0;
       while(rs.next()) {
           overlaps.remove(rs.getInt(1));
           found++;
       }
       // Index count must be the same as sequential count
       assertEquals(overlapCount,found);
       // Missing id still in overlaps map
       assertTrue(overlaps.isEmpty());
       stat.execute("drop table if exists test");
   }

    /** test in the in-memory spatial index */
    private void testMemorySpatialIndex() throws SQLException {
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        Statement stat = conn.createStatement();
        
        stat.execute("create memory table test(id int primary key, poly geometry)");
        stat.execute("create spatial index idx_test_poly on test(poly)");
        //stat.execute("create index pk on test(id)");
        stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        ResultSet rs = stat.executeQuery("explain select * from test where poly && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.IDX_TEST_POLY: POLY &&");

        
        // these queries actually have no meaning in the context of a spatial index, but 
        // check them anyhow
        stat.executeQuery("select * from test where poly = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        stat.executeQuery("select * from te44st where poly > 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        stat.executeQuery("select * from test where poly < 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");

        rs = stat.executeQuery("select * from test where poly && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        assertTrue(rs.next());
        assertEquals("POLYGON ((1 1, 1 2, 2 2, 1 1))",rs.getString("poly"));

        rs = stat.executeQuery("select * from test where poly && 'POINT (1 1)'::Geometry");
        assertTrue(rs.next());
        
        rs = stat.executeQuery("select * from test where poly && 'POINT (0 0)'::Geometry");
        assertFalse(rs.next());

        stat.execute("drop table test");
        testRandom(conn, 69, 3500);
        testRandom(conn, 44, 3500);
        conn.close();
        deleteDb("spatialIndex");
    }

}
