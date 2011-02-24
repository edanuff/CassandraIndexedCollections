package indexedcollections;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.db.marshal.Composite;
import org.apache.log4j.Logger;

/**
 * Simple indexing library using composite types
 * (https://github.com/edanuff/CassandraCompositeType) to implement indexed
 * collections in Cassandra.
 * 
 * See http://www.anuff.com/2010/07/secondary-indexes-in-cassandra.html for a
 * detailed discussion of the technique used here.
 * 
 * @author Ed Anuff
 * @see <a
 *      href="http://www.anuff.com/2010/07/secondary-indexes-in-cassandra.html">Secondary
 *      indexes in Cassandra</a>
 * @see "org.apache.cassandra.db.marshal.CompositeType"
 * 
 */
public class IndexedCollections {

	private static final Logger logger = Logger
			.getLogger(IndexedCollections.class.getName());

	public static final String DEFAULT_ITEM_CF = "Item";
	public static final String DEFAULT_CONTAINER_ITEMS_CF = "Container_Items";
	public static final String DEFAULT_CONTAINER_ITEMS_COLUMN_INDEX_CF = "Container_Items_Column_Index";
	public static final String DEFAULT_CONTAINER_ITEM_INDEX_ENTRIES = "Container_Item_Index_Entries";

	public static final CollectionCFSet defaultCFSet = new CollectionCFSet();

	public static final StringSerializer se = new StringSerializer();
	public static final ByteBufferSerializer be = new ByteBufferSerializer();
	public static final BytesArraySerializer bae = new BytesArraySerializer();
	public static final CompositeSerializer ce = new CompositeSerializer();
	public static final LongSerializer le = new LongSerializer();

	/**
	 * Sets the item column value for an item contained in a set of collections.
	 * 
	 * @param <CK>
	 *            the container's key type
	 * @param <IK>
	 *            the item's key type
	 * @param <N>
	 *            the item's column name type
	 * @param <V>
	 *            the item's column value type
	 * @param ko
	 *            the keyspace operator
	 * @param itemKey
	 *            the item row key
	 * @param columnName
	 *            the name of the column to set
	 * @param columnValue
	 *            the value to set the column to
	 * @param containers
	 *            the set of containers the item is in
	 * @param cf
	 *            the column families to use
	 * @param itemKeySerializer
	 *            the item key serializer
	 * @param nameSerializer
	 *            the column name serializer
	 * @param valueSerializer
	 *            the column value serializer
	 * @param containerKeySerializer
	 *            the container key serializer
	 */
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

			// Get all know previous index entries for this item's
			// indexed column from the item's index entry list

			SliceQuery<String, Long, Composite> q = createSliceQuery(ko, se,
					le, ce);
			q.setColumnFamily(cf.getEntries());
			q.setKey(indexEntriesKey);
			q.setRange(null, null, false, 100);
			QueryResult<ColumnSlice<Long, Composite>> r = q.execute();
			ColumnSlice<Long, Composite> slice = r.get();
			List<HColumn<Long, Composite>> results = slice.getColumns();

			// Delete all previous index entites from both the container's index
			// and the item's index entry list

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

			// Add the new index entry into the container's index and the item's
			// index entry list

			if (columnValue != null) {
				logger.info("Insert {composite(" + columnValue + ", " + itemKey
						+ ", " + timestamp + ") : " + timestamp + "} into "
						+ cf.getIndex() + "(" + columnIndexKey + ")");
				batch.addInsertion(se.toByteBuffer(columnIndexKey), cf
						.getIndex(), HFactory.createColumn(new Composite(
						columnValue, itemKey, timestamp), new byte[0], ce, bae));

				logger.info("Insert {" + timestamp + " : " + columnValue
						+ "} into " + cf.getEntries() + "(" + columnIndexKey
						+ ")");
				batch.addInsertion(se.toByteBuffer(indexEntriesKey), cf
						.getEntries(), HFactory.createColumn(timestamp,
						new Composite(columnValue), le, ce));

			}

		}

		// Store the new column value into the item
		// If new value is null, delete the value instead

		if (columnValue != null) {

			logger.info("Insert " + columnName + " : " + columnValue + " into "
					+ cf.getItem() + "(" + itemKey + ")");
			batch.addInsertion(itemKeySerializer.toByteBuffer(itemKey), cf
					.getItem(), HFactory.createColumn(columnName, columnValue,
					nameSerializer, valueSerializer));
		} else {
			batch.addDeletion(itemKeySerializer.toByteBuffer(itemKey),
					cf.getItem(), columnName, nameSerializer, timestamp);
		}

		batch.execute();

	}

	/**
	 * Search container.
	 * 
	 * @param <IK>
	 *            the item's key type
	 * @param <CK>
	 *            the container's key type
	 * @param <N>
	 *            the item's column name type
	 * @param ko
	 *            the keyspace operator
	 * @param container
	 *            the ContainerCollection (container key and collection name)
	 * @param columnName
	 *            the item's column name
	 * @param startValue
	 *            the start value for the specified column (inclusive)
	 * @param endValue
	 *            the end value for the specified column (exclusive)
	 * @param startResult
	 *            the start result row key
	 * @param count
	 *            the number of row keys to return
	 * @param reversed
	 *            search in reverse order
	 * @param cf
	 *            the column family set
	 * @param containerKeySerializer
	 *            the container key serializer
	 * @param itemKeySerializer
	 *            the item key serializer
	 * @param nameSerializer
	 *            the column name serializer
	 * @return the list of row keys for items who's column value matches
	 */
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

		Composite start = null;

		if (startValue == null) {
			if (startResult != null) {
				start = new Composite(Composite.MATCH_MINIMUM, startResult);
			} else {
				start = new Composite(Composite.MATCH_MINIMUM);
			}
		} else if (startResult != null) {
			start = new Composite(startValue, startResult);
		} else {
			start = new Composite(startValue);
		}

		Composite finish = null;

		if (endValue == null) {
			if (startValue != null) {
				finish = new Composite(startValue, Composite.MATCH_MAXIMUM);
			} else {
				finish = new Composite(Composite.MATCH_MAXIMUM);
			}
		} else {
			finish = new Composite(endValue);
		}

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

	/**
	 * Adds the item to collection.
	 * 
	 * @param <CK>
	 *            the container's key type
	 * @param <IK>
	 *            the item's key type
	 * @param <N>
	 *            the item's column name type
	 * @param <V>
	 *            the item's column value type
	 * @param ko
	 *            the keyspace operator
	 * @param container
	 *            the ContainerCollection (container key and collection name)
	 * @param itemKey
	 *            the item's row key
	 * @param cf
	 *            the column families to use
	 * @param containerKeySerializer
	 *            the container key serializer
	 * @param itemKeySerializer
	 *            the item key serializer
	 */
	public static <CK, IK, N, V> void addItemToCollection(Keyspace ko,
			ContainerCollection<CK> container, IK itemKey, CollectionCFSet cf,
			Serializer<CK> containerKeySerializer,
			Serializer<IK> itemKeySerializer) {

		createMutator(ko, se).insert(
				container.getKey(),
				cf.getItems(),
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

	/**
	 * CollectionCFSet contains the names of the four column families needed to
	 * implement indexed collections. Default CF names are provided, but can be
	 * anything that makes sense for the application.
	 */
	public static class CollectionCFSet {

		private String item = DEFAULT_ITEM_CF;
		private String items = DEFAULT_CONTAINER_ITEMS_CF;
		private String index = DEFAULT_CONTAINER_ITEMS_COLUMN_INDEX_CF;
		private String entries = DEFAULT_CONTAINER_ITEM_INDEX_ENTRIES;

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

	/**
	 * ContainerCollection represents the containing entity's key and collection
	 * name. The assumption is that an entity can have multiple collections,
	 * each with their own name.
	 * 
	 * @param <CK>
	 *            the container's row key type
	 */
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
