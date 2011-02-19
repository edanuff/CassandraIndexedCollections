package indexedcollections;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import compositecomparer.Composite;
import compositecomparer.hector.CompositeSerializer;

public class IndexedCollections {

	private static final Logger logger = Logger
			.getLogger(IndexedCollections.class.getName());

	public static final String ITEM_CF = "Item";
	public static final String CONTAINER_ITEMS_CF = "Container_Items";
	public static final String CONTAINER_ITEMS_COLUMN_INDEX_CF = "Container_Items_Column_Index";
	public static final String CONTAINER_ITEM_INDEX_ENTRIES = "Container_Item_Index_Entries";

	public static final CollectionCFSet defaultCFSet = new CollectionCFSet();

	public static final StringSerializer se = new StringSerializer();
	public static final ByteBufferSerializer be = new ByteBufferSerializer();
	public static final CompositeSerializer ce = new CompositeSerializer();
	public static final UUIDSerializer ue = new UUIDSerializer();
	public static final LongSerializer le = new LongSerializer();

	public static <CK, IK, N, V> void setItemColumn(Keyspace ko, IK itemKey,
			N columnName, V columnValue,
			Set<ContainerCollection<CK>> containers, CollectionCFSet cf,
			Serializer<IK> itemKeySerializer, Serializer<N> nameSerializer,
			Serializer<V> valueSerializer, Serializer<CK> containerKeySerializer) {

		logger.info("Set " + columnName + "='" + columnValue + "' for item "
				+ itemKey);

		long timestamp = HFactory.createClock();
		Mutator<ByteBuffer> batch = createMutator(ko, be);

		for (ContainerCollection<CK> container : containers) {

			String columnIndexKey = container.getKey() + ":"
					+ columnName.toString();

			String indexEntriesKey = container.getKey() + ":"
					+ itemKey.toString() + ":" + columnName.toString();

			SliceQuery<String, Long, Composite> q = createSliceQuery(ko, se,
					le, ce);
			q.setColumnFamily(cf.getEntries());
			q.setKey(indexEntriesKey);
			q.setRange(null, null, false, 100);
			QueryResult<ColumnSlice<Long, Composite>> r = q.execute();
			ColumnSlice<Long, Composite> slice = r.get();
			List<HColumn<Long, Composite>> results = slice.getColumns();

			for (HColumn<Long, Composite> column : results) {
				long prev_timestamp = column.getName();
				Object prev_value = column.getValue().toArray()[0];

				logger.info("Delete {" + prev_timestamp + " : " + prev_value
						+ "} from " + cf.getEntries() + "(" + columnIndexKey
						+ ")");
				batch.addDeletion(se.toByteBuffer(indexEntriesKey),
						cf.getEntries(), column.getName(), le, timestamp);

				logger.info("Delete composite(" + prev_value + ", " + itemKey
						+ ", " + prev_timestamp + ") from " + cf.getIndex()
						+ "(" + columnIndexKey + ")");
				batch.addDeletion(se.toByteBuffer(columnIndexKey), cf
						.getIndex(), new Composite(prev_value, itemKey,
						prev_timestamp), ce, timestamp);

			}

			if (columnValue != null) {
				logger.info("Insert {composite(" + columnValue + ", " + itemKey
						+ ", " + timestamp + ") : " + timestamp + "} into "
						+ cf.getIndex() + "(" + columnIndexKey + ")");
				batch.addInsertion(se.toByteBuffer(columnIndexKey), cf
						.getIndex(), HFactory.createColumn(new Composite(
						columnValue, itemKey, timestamp), timestamp, ce, le));

				logger.info("Insert {" + timestamp + " : " + columnValue
						+ "} into " + cf.getEntries() + "(" + columnIndexKey
						+ ")");
				batch.addInsertion(se.toByteBuffer(indexEntriesKey), cf
						.getEntries(), HFactory.createColumn(timestamp,
						new Composite(columnValue), le, ce));

			}

		}

		if (columnValue != null) {

			logger.info("Insert " + columnName + " : " + columnValue + " into "
					+ cf.getItem() + "(" + itemKey + ")");
			batch.addInsertion(itemKeySerializer.toByteBuffer(itemKey), cf
					.getItem(), HFactory.createColumn(columnName, columnValue,
					nameSerializer, valueSerializer));
		}

		batch.execute();

	}

	public static <IK, CK, N> List<IK> searchContainer(Keyspace ko,
			ContainerCollection<CK> container, N columnName, Object startValue,
			Object endValue, IK startResult, int count, boolean reversed,
			CollectionCFSet cf, Serializer<CK> containerKeySerializer,
			Serializer<IK> itemKeySerializer, Serializer<N> nameSerializer) {
		List<IK> items = new ArrayList<IK>();

		String columnIndexKey = container.getKey() + ":"
				+ columnName.toString();

		SliceQuery<ByteBuffer, Composite, ByteBuffer> q = createSliceQuery(ko,
				be, ce, be);
		q.setColumnFamily(cf.getIndex());
		q.setKey(se.toByteBuffer(columnIndexKey));

		if (startValue == null) {
			startValue = new byte[0];
		}
		if (endValue == null) {
			endValue = startValue;
		}

		Composite start = null;
		if (startResult != null) {
			start = new Composite(startValue, startResult);
		} else {
			start = new Composite(startValue);
		}

		Composite finish = new Composite(endValue, Composite.MATCH_MAXIMUM);

		q.setRange(start, finish, reversed, count);
		QueryResult<ColumnSlice<Composite, ByteBuffer>> r = q.execute();
		ColumnSlice<Composite, ByteBuffer> slice = r.get();
		List<HColumn<Composite, ByteBuffer>> results = slice.getColumns();

		if (results != null) {
			for (HColumn<Composite, ByteBuffer> result : results) {
				IK key = getAsType(result.getName().toArray()[1],
						itemKeySerializer);
				if (key != null) {
					items.add(key);
				}
			}
		}

		return items;
	}

	public static <CK, IK, N, V> void addItemToCollection(Keyspace ko,
			ContainerCollection<CK> container, IK itemKey, CollectionCFSet cf,
			Serializer<CK> containerKeySerializer,
			Serializer<IK> itemKeySerializer) {

		createMutator(ko, se).insert(
				container.getKey(),
				IndexedCollections.CONTAINER_ITEMS_CF,
				createColumn(itemKey, HFactory.createClock(),
						itemKeySerializer, le));

	}

	@SuppressWarnings("unchecked")
	public static <T, K> T getAsType(K obj, Serializer<T> st) {
		Serializer<K> so = SerializerTypeInferer.getSerializer(obj);
		if (so == null) {
			return null;
		}
		if (so.getClass().equals(st.getClass())) {
			return (T) obj;
		}
		return st.fromByteBuffer(so.toByteBuffer(obj));
	}

	public static class CollectionCFSet {

		private String item = ITEM_CF;
		private String items = CONTAINER_ITEMS_CF;
		private String index = CONTAINER_ITEMS_COLUMN_INDEX_CF;
		private String entries = CONTAINER_ITEM_INDEX_ENTRIES;

		public CollectionCFSet() {
		}

		public CollectionCFSet(String item, String items, String index,
				String entries) {
			this.item = item;
			this.items = items;
			this.index = index;
			this.entries = entries;
		}

		public String getItem() {
			return item;
		}

		public void setItem(String item) {
			this.item = item;
		}

		public String getItems() {
			return items;
		}

		public void setItems(String items) {
			this.items = items;
		}

		public String getIndex() {
			return index;
		}

		public void setIndex(String index) {
			this.index = index;
		}

		public String getEntries() {
			return entries;
		}

		public void setEntries(String entries) {
			this.entries = entries;
		}
	}

	public static class ContainerCollection<CK> {
		private CK ownerKey;
		private String collectionName;

		public ContainerCollection(CK ownerKey, String collectionName) {
			this.ownerKey = ownerKey;
			this.collectionName = collectionName;
		}

		public CK getOwnerKey() {
			return ownerKey;
		}

		public void setOwnerKey(CK ownerKey) {
			this.ownerKey = ownerKey;
		}

		public String getCollectionName() {
			return collectionName;
		}

		public void setCollectionName(String collectionName) {
			this.collectionName = collectionName;
		}

		public String getKey() {
			return ownerKey + ":" + collectionName;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((collectionName == null) ? 0 : collectionName.hashCode());
			result = prime * result
					+ ((ownerKey == null) ? 0 : ownerKey.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			@SuppressWarnings("rawtypes")
			ContainerCollection other = (ContainerCollection) obj;
			if (collectionName == null) {
				if (other.collectionName != null) {
					return false;
				}
			} else if (!collectionName.equals(other.collectionName)) {
				return false;
			}
			if (ownerKey == null) {
				if (other.ownerKey != null) {
					return false;
				}
			} else if (!ownerKey.equals(other.ownerKey)) {
				return false;
			}
			return true;
		}
	}
}
