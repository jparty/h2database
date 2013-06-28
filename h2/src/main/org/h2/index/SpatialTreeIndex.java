/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.List;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * This is an in-memory index based on a R-Tree.
 */
public class SpatialTreeIndex extends BaseIndex implements SpatialIndex {

    private MVRTreeMap<Long> treeMap;
    private MVStore store;

    private final RegularTable tableData;
    private long rowCount;
    private boolean closed;

    public SpatialTreeIndex(RegularTable table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        if (indexType.isUnique()) {
            throw DbException.getUnsupportedException("not unique");
        }
        if (columns.length > 1) {
            throw DbException.getUnsupportedException("can only do one column");
        }
        if ((columns[0].sortType & SortOrder.DESCENDING) != 0) {
            throw DbException.getUnsupportedException("cannot do descending");
        }
        if ((columns[0].sortType & SortOrder.NULLS_FIRST) != 0) {
            throw DbException.getUnsupportedException("cannot do nulls first");
        }
        if ((columns[0].sortType & SortOrder.NULLS_LAST) != 0) {
            throw DbException.getUnsupportedException("cannot do nulls last");
        }

        initBaseIndex(table, id, indexName, columns, indexType);
        tableData = table;
        if (!database.isStarting()) {
            if (columns[0].column.getType() != Value.GEOMETRY) {
                throw DbException.getUnsupportedException("spatial index on non-geometry column, "
                        + columns[0].column.getCreateSQL());
            }
        }
        store = MVStore.open(null);
        treeMap =  store.openMap("spatialIndex",
                new MVRTreeMap.Builder<Long>());
    }

    @Override
    public void close(Session session) {
        store.close();
        closed = true;
    }

    @Override
    public void add(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        treeMap.add(getEnvelope(row),row.getKey());
        rowCount++;
    }
    
    private SpatialKey getEnvelope(SearchRow row) {
        Value v = row.getValue(columnIds[0]);
        Geometry g = ((ValueGeometry) v).getGeometry();
        Envelope env = g.getEnvelopeInternal();
        return new SpatialKey(row.getKey(),castDouble(env.getMinX(),false),castDouble(env.getMaxX(),true),
                castDouble(env.getMinY(),false),castDouble(env.getMaxY(),true));
    }

    /**
     * Cast the provided value to float and set an offset equal to the approximation error.
     * @param value
     * @param upperPrecision If true the offset is added to the value, else it is removed to the value.
     * @return Casted value
     */
    private static float castDouble(double value, boolean upperPrecision) {
        double epsilon = Math.abs (value)-Math.abs((float)value);
        return (float)(value+(upperPrecision ? epsilon : -epsilon));
    }

    @Override
    public void remove(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        if (!treeMap.remove(getEnvelope(row),row.getKey())) {
            throw DbException.throwInternalError("row not found");
        }
        rowCount--;
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find(filter.getSession());
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session);
    }

    private Cursor find(Session session) {
        return new ListCursor(treeMap.keyIterator(), true/*first*/,tableData,session);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Cursor findByGeometry(TableFilter filter, SearchRow intersection) {
        // FIXME: ideally I need external iterators, but let's see if we can get
        // it working first
        java.util.List<Long> list;
        if (intersection != null) {
            list = root.query(getEnvelope(intersection));
        } else {
            list = root.queryAll();
        }
        return new ListCursor(list, true/*first*/, tableData, filter.getSession());
    }

    @Override
    protected long getCostRangeIndex(int[] masks, long rowCount, SortOrder sortOrder) {
        rowCount += Constants.COST_ROW_OFFSET;
        long cost = rowCount;
        long rows = rowCount;
        if (masks == null) {
            return cost;
        }
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.OVERLAP) == IndexCondition.OVERLAP) {
                cost = 3 + rows / 4;
            }
        }
        return cost;
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        return getCostRangeIndex(masks, tableData.getRowCountApproximation(), sortOrder);
    }

    @Override
    public void remove(Session session) {
        truncate(session);
    }

    @Override
    public void truncate(Session session) {
        root = null;
        rowCount = 0;
    }

    @Override
    public void checkRename() {
        // nothing to do
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        if (closed) {
            throw DbException.throwInternalError();
        }

        // FIXME: ideally I need external iterators, but let's see if we can get
        // it working first
        @SuppressWarnings("unchecked")
        List<Long> list = root.queryAll();
        
        return new ListCursor(list, first,tableData,session);
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    private static final class ListCursor implements Cursor {
        private final List<Long> rows;
        private int index;
        private Row current;
        private final RegularTable tableData;
        private Session session;

        public ListCursor(List<Long> rows, boolean first, RegularTable tableData, Session session) {
            this.rows = rows;
            this.index = first ? 0 : rows.size();
            this.tableData = tableData;
            this.session = session;
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public SearchRow getSearchRow() {
            return current;
        }

        @Override
        public boolean next() {
            current = index >= rows.size() ? null : tableData.getRow(session, rows.get(index++));
            return current != null;
        }

        @Override
        public boolean previous() {
            current = index < 0 ? null : tableData.getRow(session, rows.get(index--));
            return current != null;
        }

    }

}
