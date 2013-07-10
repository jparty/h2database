package org.h2.index;

import com.vividsolutions.jts.index.strtree.STRtree;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
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

import java.util.List;

/**
 * This is an in-memory index based on a R-Tree.
 * @author Noël Grandin
 * @author Nicolas Fortin IRSTV FR CNRS 24888
 */
public class SpatialMemoryTreeIndex extends BaseIndex implements SpatialIndex {

    private STRtree root;

    private final RegularTable tableData;
    private long rowCount;
    private boolean closed;

    public SpatialMemoryTreeIndex(RegularTable table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
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

        root = new STRtree(12);
    }

    @Override
    public void close(Session session) {
        root = null;
        closed = true;
    }

    @Override
    public void add(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        root.insert(getEnvelope(row), row.getKey());
        rowCount++;
    }

    private Envelope getEnvelope(SearchRow row) {
        Value v = row.getValue(columnIds[0]);
        Geometry g = ((ValueGeometry) v).getGeometry();
        return g.getEnvelopeInternal();
    }

    @Override
    public void remove(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        if (!root.remove(getEnvelope(row), row.getKey())) {
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
        // FIXME: ideally I need external iterators, but let's see if we can get
        // it working first
        // FIXME: in the context of a spatial index, a query that uses ">" or "<" has no real meaning, so for now just ignore
        // it and return all rows
        List<Object> list = root.itemsTree();
        return new ListCursor(list, true/*first*/,tableData,session);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Cursor findByGeometry(TableFilter filter, SearchRow intersection) {
        // FIXME: ideally I need external iterators, but let's see if we can get
        // it working first
        List<Object> list;
        if (intersection != null) {
            list = root.query(getEnvelope(intersection));
        } else {
            list = root.itemsTree();
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
        List<Object> list = root.itemsTree();
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
        private final List<Object> rows;
        private int index;
        private Row current;
        private final RegularTable tableData;
        private Session session;

        public ListCursor(List<Object> rows, boolean first, RegularTable tableData, Session session) {
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
            current = null;
            while(index < rows.size()) {
                Object object = rows.get(index++);
                if(object instanceof Long) {
                    current = tableData.getRow(session, (Long)object);
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean previous() {
            current = null;
            while(index >= 0) {
                Object object = rows.get(index--);
                if(object instanceof Long) {
                    current = tableData.getRow(session, (Long)object);
                    return true;
                }
            }
            return false;
        }

    }

}
