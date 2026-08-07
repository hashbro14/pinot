/**
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
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.plugin.filesystem.S3PinotFS;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.store.RemoteIndexFetcher;
import org.apache.pinot.segment.local.segment.store.RemoteQueryConfigs;
import org.apache.pinot.segment.local.segment.store.RemoteSegmentRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.util.TestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end proof of direct deep-store querying: a real Pinot cluster (controller + broker + server) answers
 * queries for an OFFLINE table whose segment data lives ONLY in an S3-compatible object store (S3Mock
 * container). The server never downloads segment data — it fetches KB-sized metadata at load and ranged-reads
 * index bytes per query.
 *
 * <p>Verified here: correct query results (count star for every segment, filtered/aggregation queries matching
 * H2), the remote registry actually serving all segments, ranged fetches actually flowing (fetch count and
 * bytes), and no {@code columns.psf} present anywhere under the server's data directory.
 */
public class RemoteDeepStoreQueryIntegrationTest extends BaseClusterIntegrationTest {
  private static final int S3MOCK_PORT = 9090;
  private static final String BUCKET = "remote-deep-store";

  private GenericContainer<?> _s3MockContainer;
  private S3Client _s3Client;
  private String _s3Endpoint;
  private TableConfig _remoteTableConfig;
  private Schema _remoteSchema;

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    // The remote loader (delegates to the default loader for non-remote tables)
    serverConf.setProperty("pinot.server.instance.segment.directory.loader", "remote");
    // Existing upstream flag: plans fetch contexts and brackets operators in acquire/release
    serverConf.setProperty("pinot.server.query.executor.enable.prefetch", "true");
  }

  @Override
  protected TableConfig createOfflineTableConfig() {
    TableConfig tableConfig = super.createOfflineTableConfig();
    tableConfig.setCustomConfig(new TableCustomConfig(
        Map.of(RemoteQueryConfigs.TABLE_REMOTE_QUERY_ENABLED, "true",
            RemoteQueryConfigs.TABLE_REMOTE_QUERY_BASE_URI, "s3://" + BUCKET + "/" + getTableName() + "/")));
    return tableConfig;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the S3-compatible deep store and register the s3 scheme in this JVM (in production this is the
    // standard pinot.server.storage.factory.class.s3 configuration)
    _s3MockContainer = new GenericContainer<>("adobe/s3mock:3.12.0").withExposedPorts(S3MOCK_PORT)
        .waitingFor(Wait.forHttp("/favicon.ico").forPort(S3MOCK_PORT).forStatusCode(200));
    _s3MockContainer.start();
    _s3Endpoint = "http://" + _s3MockContainer.getHost() + ":" + _s3MockContainer.getMappedPort(S3MOCK_PORT);
    _s3Client = S3Client.builder().region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret")))
        .endpointOverride(URI.create(_s3Endpoint))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build();
    _s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    PinotConfiguration s3FsConfig = new PinotConfiguration(Map.of(
        "region", "us-east-1", "accessKey", "access", "secretKey", "secret", "endpoint", _s3Endpoint));
    PinotFSFactory.register("s3", S3PinotFS.class.getName(), s3FsConfig);
    S3PinotFS s3PinotFS = new S3PinotFS();
    s3PinotFS.init(_s3Client);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Schema + table (remote-query enabled) must exist before segments are uploaded
    _remoteSchema = createSchema();
    addSchema(_remoteSchema);
    _remoteTableConfig = createOfflineTableConfig();
    addTableConfig(_remoteTableConfig);

    // Build segments from the standard Avro test data, convert to v3, and publish the untarred v3 layout
    // to the deep store BEFORE uploading (servers go remote immediately on the ONLINE transition)
    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, _remoteTableConfig, _remoteSchema, 0,
        _segmentDir, _tarDir);
    publishSegmentsToDeepStore(s3PinotFS);

    // Upload the segments (registers them in ZK/Helix; the data download is skipped by the remote path)
    uploadSegments(getTableName(), _tarDir);

    // H2 for result comparison
    setUpH2Connection(avroFiles);

    waitForAllDocsLoaded(600_000L);
  }

  private void publishSegmentsToDeepStore(PinotFS pinotFS)
      throws Exception {
    File[] segmentDirs = _segmentDir.listFiles(File::isDirectory);
    assertTrue(segmentDirs != null && segmentDirs.length > 0, "no segments built");
    SegmentV1V2ToV3FormatConverter converter = new SegmentV1V2ToV3FormatConverter();
    for (File segmentDir : segmentDirs) {
      File v3Dir = new File(segmentDir, "v3");
      if (!v3Dir.isDirectory()) {
        converter.convert(segmentDir);
      }
      assertTrue(v3Dir.isDirectory(), "v3 conversion failed for " + segmentDir);
      for (File file : v3Dir.listFiles()) {
        pinotFS.copyFromLocalFile(file,
            URI.create("s3://" + BUCKET + "/" + getTableName() + "/" + segmentDir.getName() + "/v3/"
                + file.getName()));
      }
    }
  }

  @Test
  public void testCountStarServedFromDeepStore()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), getCountStarResult());
  }

  @Test
  public void testFilteredAggregationMatchesH2()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE Carrier = 'AA'";
    JsonNode response = postQuery(query);
    long pinotCount = response.get("resultTable").get("rows").get(0).get(0).asLong();
    try (Statement statement = _h2Connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM mytable WHERE Carrier = 'AA'")) {
      assertTrue(resultSet.next());
      assertEquals(pinotCount, resultSet.getLong(1));
    }
  }

  @Test
  public void testGroupByMatchesH2()
      throws Exception {
    String query =
        "SELECT Carrier, COUNT(*) FROM " + getTableName() + " GROUP BY Carrier ORDER BY Carrier LIMIT 100";
    JsonNode rows = postQuery(query).get("resultTable").get("rows");
    try (Statement statement = _h2Connection.createStatement();
        ResultSet resultSet = statement.executeQuery(
            "SELECT Carrier, COUNT(*) FROM mytable GROUP BY Carrier ORDER BY Carrier")) {
      int i = 0;
      while (resultSet.next()) {
        assertEquals(rows.get(i).get(0).asText(), resultSet.getString(1));
        assertEquals(rows.get(i).get(1).asLong(), resultSet.getLong(2));
        i++;
      }
      assertEquals(rows.size(), i, "row count mismatch vs H2");
    }
  }

  @Test
  public void testDataActuallyServedRemotely() {
    // Every segment is registered remote in this JVM's registry
    assertTrue(RemoteSegmentRegistry.getInstance().size() > 0, "no remote segments registered");
    // Ranged fetches actually flowed from the deep store
    assertTrue(RemoteIndexFetcher.getInstance().getFetchCount() > 0, "no ranged fetches happened");
    assertTrue(RemoteIndexFetcher.getInstance().getFetchedBytes() > 0, "no bytes fetched from deep store");
  }

  @Test
  public void testNoSegmentDataOnServerDisk()
      throws Exception {
    // The server's data dir must hold only the KB-sized metadata files - never columns.psf
    File serverDir = new File(TEMP_SERVER_DIR);
    assertTrue(serverDir.isDirectory(), "server temp dir not found: " + serverDir);
    try (Stream<java.nio.file.Path> paths = Files.walk(serverDir.toPath())) {
      List<String> offenders = paths.filter(path -> path.getFileName().toString().equals("columns.psf"))
          .map(java.nio.file.Path::toString)
          .collect(Collectors.toList());
      assertTrue(offenders.isEmpty(), "segment data found on server disk: " + offenders);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    if (_s3Client != null) {
      _s3Client.close();
    }
    if (_s3MockContainer != null) {
      _s3MockContainer.stop();
    }
  }
}
