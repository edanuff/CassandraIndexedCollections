package indexedcollections;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import indexedcollections.IndexedCollections.ContainerCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import comparators.hector.CompositeSerializer;

/**
 * Example class showing usage of IndexedCollections.
 */
public class IndexTest {

	private static final Logger logger = Logger.getLogger(IndexTest.class
			.getName());

	public static final String KEYSPACE = "Keyspace";

	public static final StringSerializer se = new StringSerializer();
	public static final ByteBufferSerializer be = new ByteBufferSerializer();
	public static final CompositeSerializer ce = new CompositeSerializer();
	public static final UUIDSerializer ue = new UUIDSerializer();
	public static final LongSerializer le = new LongSerializer();

	static EmbeddedServerHelper embedded;

	static Cluster cluster;
	static Keyspace ko;

	@BeforeClass
	public static void setup() throws TTransportException, IOException,
			InterruptedException, ConfigurationException {
		embedded = new EmbeddedServerHelper();
		embedded.setup();

		cluster = getOrCreateCluster("MyCluster", "127.0.0.1:9170");
		ko = createKeyspace(KEYSPACE, cluster);

		ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);

		setupColumnFamilies(cfDefList);

		makeKeyspace(cluster, KEYSPACE,
				"org.apache.cassandra.locator.SimpleStrategy", 1, cfDefList);

	}

	@AfterClass
	public static void teardown() throws IOException {
		embedded.teardown();
		embedded = null;
	}

	public static void setupColumnFamilies(List<CfDef> cfDefList) {

		createCF(IndexedCollections.DEFAULT_ITEM_CF,
				BytesType.class.getSimpleName(), cfDefList);

		createCF(IndexedCollections.DEFAULT_CONTAINER_ITEMS_CF,
				TimeUUIDType.class.getSimpleName(), cfDefList);

		createCF(IndexedCollections.DEFAULT_CONTAINER_ITEMS_COLUMN_INDEX_CF,
				"comparators.CompositeType", cfDefList);

		createCF(IndexedCollections.DEFAULT_CONTAINER_ITEM_INDEX_ENTRIES,
				LongType.class.getSimpleName(), cfDefList);

	}

	public static void createCF(String name, String comparator_type,
			List<CfDef> cfDefList) {
		cfDefList.add(new CfDef(KEYSPACE, name)
				.setComparator_type(comparator_type).setKey_cache_size(0)
				.setRow_cache_size(0).setGc_grace_seconds(86400));
	}

	public static void makeKeyspace(Cluster cluster, String name,
			String strategy, int replicationFactor, List<CfDef> cfDefList) {

		if (cfDefList == null) {
			cfDefList = new ArrayList<CfDef>();
		}

		try {
			KsDef ksDef = new KsDef(name, strategy, replicationFactor,
					cfDefList);
			cluster.addKeyspace(new ThriftKsDef(ksDef));
			return;
		} catch (Throwable e) {
			logger.error("Exception while creating keyspace, " + name
					+ " - probably already exists", e);
		}

		for (CfDef cfDef : cfDefList) {
			try {
				cluster.addColumnFamily(new ThriftCfDef(cfDef));
			} catch (Throwable e) {
				logger.error("Exception while creating CF, " + cfDef.getName()
						+ " - probably already exists", e);
			}
		}
	}

	public static java.util.UUID newTimeUUID() {
		com.eaio.uuid.UUID eaioUUID = new com.eaio.uuid.UUID();
		return new UUID(eaioUUID.time, eaioUUID.clockSeqAndNode);
	}

	public UUID createEntity(String type) {
		UUID id = newTimeUUID();
		createMutator(ko, ue).insert(id, IndexedCollections.DEFAULT_ITEM_CF,
				createColumn("type", type, se, se));
		return id;
	}

	public void addEntityToCollection(ContainerCollection<UUID> container,
			UUID itemEntity) {
		IndexedCollections.addItemToCollection(ko, container, itemEntity,
				IndexedCollections.defaultCFSet, ue, ue);
	}

	@Test
	public void testIndexes() throws IOException, TTransportException,
			InterruptedException, ConfigurationException {

		UUID g1 = createEntity("company");
		ContainerCollection<UUID> container = new ContainerCollection<UUID>(g1,
				"employees");
		Set<ContainerCollection<UUID>> containers = new LinkedHashSet<ContainerCollection<UUID>>();
		containers.add(container);

		UUID e1 = createEntity("employee");
		UUID e2 = createEntity("employee");
		UUID e3 = createEntity("employee");

		addEntityToCollection(container, e1);
		addEntityToCollection(container, e2);
		addEntityToCollection(container, e3);

		IndexedCollections.setItemColumn(ko, e1, "name", "bob", containers,
				IndexedCollections.defaultCFSet, ue, se, se, ue);

		IndexedCollections.setItemColumn(ko, e2, "name", "fred", containers,
				IndexedCollections.defaultCFSet, ue, se, se, ue);

		IndexedCollections.setItemColumn(ko, e3, "name", "bill", containers,
				IndexedCollections.defaultCFSet, ue, se, se, ue);

		logger.info("Select where name='fred'");

		List<UUID> results = IndexedCollections.searchContainer(ko, container,
				"name", "fred", null, null, 100, false,
				IndexedCollections.defaultCFSet, ue, ue, se);

		logger.info(results.size() + " results found");

		assertEquals(1, results.size());
		assertTrue(results.get(0).equals(e2));

		logger.info("Result found is " + results.get(0));

		IndexedCollections.setItemColumn(ko, e2, "name", "steve", containers,
				IndexedCollections.defaultCFSet, ue, se, se, ue);

		logger.info("Select where name='fred'");

		results = IndexedCollections.searchContainer(ko, container, "name",
				"fred", null, null, 100, false,
				IndexedCollections.defaultCFSet, ue, ue, se);

		logger.info(results.size() + " results found");

		assertEquals(0, results.size());

		logger.info("Select where name>='bill' and name<'c'");

		results = IndexedCollections.searchContainer(ko, container, "name",
				"bill", "c", null, 100, false, IndexedCollections.defaultCFSet,
				ue, ue, se);

		logger.info(results.size() + " results found");

		assertEquals(2, results.size());

	}

}
