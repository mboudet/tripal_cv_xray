<?php

namespace Tests;

use StatonLab\TripalTestSuite\DBTransaction;
use StatonLab\TripalTestSuite\TripalTestCase;

class XrayIndexerJobTest extends TripalTestCase {

  use DBTransaction;

  /**
   * The mRNA bundle object.
   *
   * @var object
   */
  private $bundle;

  /**
   * The created entity id.
   *
   * @var int
   */
  private $entity_id;

  /**
   * The mRNA feature.
   *
   * @var object
   */
  private $mrna;

  /**
   * mRNA cvterm.
   *
   * @var object
   */
  private $mrna_term;

  /**
   * Set up the test.
   *
   * @throws \Exception
   */
  public function setUp() {
    parent::setUp();

    $this->publishAnnotatedFeatureEntities();
  }

  /**
   * Creates and publishes GO annotated mRNA entities.
   *
   * @throws \Exception
   */
  private function publishAnnotatedFeatureEntities() {
    // Pick three go terms that are all nearby
    $termA = chado_get_cvterm(['id' => 'GO:0003723']);
    $termB = chado_get_cvterm(['id' => 'GO:0008135']);
    $termC = chado_get_cvterm(['id' => 'GO:0003677']);

    $terms = [
      $termA,
      $termB,
      $termC,
    ];

    $this->mrna_term = chado_get_cvterm(['id' => 'SO:0000234']);
    $this->mrna = factory('chado.feature')->create(['type_id' => $this->mrna_term->cvterm_id]);

    foreach ($terms as $term) {
      factory('chado.feature_cvterm')->create([
        'feature_id' => $this->mrna->feature_id,
        'cvterm_id' => $term->cvterm_id,
      ]);
    }

    $this->publish('feature');

    $this->entity_id = chado_get_record_entity_by_table('feature', $this->mrna->feature_id);

    if (!$this->entity_id) {
      print("Entity could not be published!");
      return;
    }

    $bundle_id = db_select('chado_bundle', 'cb')
      ->fields('cb', ['bundle_id'])
      ->condition('type_id', $this->mrna_term->cvterm_id)
      ->execute()
      ->fetchField();

    $this->bundle = tripal_load_bundle_entity(['id' => $bundle_id]);
    $this->bundle->bundle_id = $bundle_id;
  }

  /**
   * The setUp method publishes annotated entities that trigger the entity
   * insert hook. This test makes sure the hook got triggered and that
   * the items appear in the index after the job gets executed.
   *
   * @throws \Exception
   */
  public function testInsertHookIsTriggered() {
    // Verify that our job got inserted into the queue
    /** @var \SystemQueue $queue */
    $queue = \DrupalQueue::get('tripal_cv_xray');
    $this->assertGreaterThan(0, $queue->numberOfItems());

    // Run all jobs in the queue
    while ($item = $queue->claimItem()) {
      /** @var \XRayIndexerJob $job */
      $job = $item->data;
      $job->handle();
    }

    // Verify the entity exists in the index
    $count = db_select('tripal_cvterm_entity_linker', 'TCEL')
      ->condition('entity_id', $this->entity_id)
      ->countQuery()
      ->execute()
      ->fetchField();
    $this->assertGreaterThan(0, (int) $count);
  }

  /**
   * @ticket 39
   * @group failing
   * The indexer currently ends up with many identical entries: for example,
   *   this entity https://hardwoodgenomics.org/bio_data/132373
   * @throws \Exception
   */
  public function test_XrayIndexerNoDuplicate() {
    $job = new \XRayIndexerJob([
      'bundle' => $this->bundle,
      'cv_shortnames' => ['GO'],
    ]);
    $job->offset(0);
    $job->limit(500000);
    $job->handle();

    $query = 'SELECT t.cvterm_id AS id, count(*) AS count FROM {tripal_cvterm_entity_linker} t 
              WHERE t.entity_id = :entity_id
              GROUP BY t.cvterm_id';

    $results = db_query($query, [':entity_id' => $this->entity_id])->fetchAll();

    /**
     * For any given entity, a given cvterm should not be in the linker more than once.
     */
    foreach ($results as $result) {
      $this->assertLessThan(2, (int) $result->count);
    }
  }
}
