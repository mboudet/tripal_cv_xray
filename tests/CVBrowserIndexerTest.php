<?php

namespace Tests;

use StatonLab\TripalTestSuite\DBTransaction;
use StatonLab\TripalTestSuite\TripalTestCase;

class CVBrowserIndexerTest extends TripalTestCase {

  use DBTransaction;

  /** @test */
  public function testIndexerGetsBundles() {
    $indexer = new \CVBrowserIndexer();

    $bundles = $indexer->bundles();

    $this->assertNotEmpty($bundles);
    $bundle = current($bundles);
    $this->assertObjectHasAttribute('bundle_id', $bundle);
    $this->assertObjectHasAttribute('data_table', $bundle);
  }

  /** @test */
  public function testIndexerGetsTotalRecordInBundles() {
    $indexer = new \CVBrowserIndexer();

    $bundle = current($indexer->bundles());
    $total = $indexer->bundleTotal($bundle);

    $this->assertTrue(is_int($total));
  }

  /** @test */
  public function testChunkingHandlesPositionCorrectly() {
    $indexer = new \CVBrowserIndexer();

    $bundle = current($indexer->bundles());
    $total = $indexer->bundleTotal($bundle);
    $chunk_size = 100;
    $indexer->setChunkSize($chunk_size);
    $position = 0;
    $chunk = $indexer->getEntitiesChunk($bundle, $total, $position);
    $this->assertEquals($position + $chunk_size, $chunk_size);
    $this->assertTrue(count($chunk) <= $chunk_size);
  }

  /** @test */
  public function testGettingPrimaryKey() {
    $indexer = new \CVBrowserIndexer();
    $key = $indexer->primaryKey('feature');

    $this->assertEquals($key, 'feature_id');
  }

  /** @test */
  public function testLoadingData() {
    $indexer = new \CVBrowserIndexer();
    $indexer->clearIndexTable();
    $indexer->index(true);

    $count = db_query('SELECT COUNT(*) FROM tripal_cvterm_entity_linker')->fetchField();
    $this->assertTrue($count > 0);
  }
}
