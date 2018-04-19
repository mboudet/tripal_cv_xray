<?php

namespace Tests;

use StatonLab\TripalTestSuite\DBTransaction;
use StatonLab\TripalTestSuite\TripalTestCase;

class CVBrowserIndexerTest extends TripalTestCase {

  use DBTransaction;

//  /** @test */
//  public function testIndexerGetsBundles() {
//    $indexer = new \CVBrowserIndexer();
//
//    $bundles = $indexer->bundles();
//
//    $this->assertNotEmpty($bundles);
//    $bundle = $bundles->fetchObject();
//    $this->assertObjectHasAttribute('bundle_id', $bundle);
//    $this->assertObjectHasAttribute('data_table', $bundle);
//  }
//
//  /** @test */
//  public function testIndexerGetsTotalRecordInBundles() {
//    $indexer = new \CVBrowserIndexer();
//
//    $bundle =$indexer->bundles();
//    $total = $indexer->bundleTotal($bundle->fetchObject());
//
//    $this->assertTrue(is_int($total));
//  }
//
//  /** @test */
//  public function testChunkingHandlesPositionCorrectly() {
//    $indexer = new \CVBrowserIndexer();
//
//    $bundle = $indexer->bundles()->fetchObject();
//    $indexer->bundleTotal($bundle);
//    $chunk_size = 100;
//    $indexer->setChunkSize($chunk_size);
//    $position = 0;
//    $indexer->getEntitiesChunk($bundle, $position);
//    $this->assertEquals($position + $chunk_size, $chunk_size);
//  }
//
//  /** @test */
//  public function testGettingPrimaryKey() {
//    $indexer = new \CVBrowserIndexer();
//    $key = $indexer->primaryKey('feature');
//
//    $this->assertEquals($key, 'feature_id');
//  }

  /** @test */
  public function testIndexingData() {
    $indexer = new \CVBrowserIndexer();
    $indexer->clearIndexTable();
    $indexer->setChunkSize(5);
    $indexer->index(true);

    $count = (int) db_query('SELECT COUNT(*) FROM tripal_cvterm_entity_linker')->fetchField();
    $this->assertTrue($count > 0);
  }
}
