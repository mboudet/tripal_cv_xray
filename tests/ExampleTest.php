<?php

namespace Tests;

use StatonLab\TripalTestSuite\DBTransaction;
use StatonLab\TripalTestSuite\TripalTestCase;

/**
 * Class ExampleTest
 *
 * Note that test classes must have a suffix of Test.php and the filename
 * must match the class name.
 *
 * @package Tests
 */
class ExampleTest extends TripalTestCase {

  /** @test */
  public function testBasicExample() {
    $dispatcher = new \XRayDispatcherJob(['6' , '7', '8'], ["GO"],  100);
    $dispatcher->clearIndexTable();

    $count = (int) db_query('SELECT COUNT(*) FROM tripal_cvterm_entity_linker')->fetchField();
    $this->assertEquals($count, 0);

    $bundles = $dispatcher->bundles();
    foreach ($bundles as $bundle) {
      echo "\nProcessing: chado_bio_data_{$bundle->bundle_id}\n";
      $total = $dispatcher->bundleTotal($bundle);
      for ($i = 0; $i < $total; $i += 1) {
        $job = new \XRayIndexerJob($bundle, ["GO"], TRUE);
        $job->offset($i)->limit(1);
        $job->handle();
      }
    }

    $count = (int) db_query('SELECT COUNT(*) FROM tripal_cvterm_entity_linker')->fetchField();
    echo "FINAL COUNT $count\n";
  }
}

//
//  public function test_get_all_associated_cvterms() {
//
//    //look up a random record ID with a prop
//    $query = db_select("chado.feature", 'F')
//      ->fields('F', ['feature_id']);
//    $query->join('chado.featureprop', 'FP', 'F.feature_id = FP.feature_id');
//
//    $record_id = $query->execute()->fetchObject()->feature_id;
//
//  }
//
//  public function test_associate_entity_across_ancestors() {
//
//    //look up a random record ID children in cvtermpath
//    $query = db_select("chado.featureprop", 'FP')
//      ->fields('FP', ['type_id']);
//
//    $query->join('chado.cvtermpath', 'CVTP', 'FP.type_id = CVTP.subject_id');
//
//    $cvterm_id = $query->execute()->fetchObject();
//
//    if (!$cvterm_id) {
//      print ("\nwarning: No featureprops with relational cvterms to check\n");
//
//      return;
//    }
//
//  }
//
//
//  //WARNING: this test runs the update all entities job!
//  //It's going to take a long time!  Run it to populate your entity term index...
//  public function test_creation_linked_features() {
//
//    //look up cvterm for mrna
//
//    $mrna_id = db_select("chado.cvterm", "CVT")
//      ->fields('CVT', ['cvterm_id'])
//      ->condition('CVT.name', 'mRNA')
//      ->execute()->fetchField();
//
//
//    $organism = factory('chado.organism')->create();
//
//    $features = factory('chado.feature', 100)->create([
//      'organism_id' => $organism->organism_id,
//      'type_id' => $mrna_id,
//    ]);
//
//    $related_terms = $this->set_fake_cvterm_relationships();
//    //randomly assign these features some props with relationships
//
//    factory('chado.feature_cvterm')->create();
//
//    foreach ($features as $feature) {
//      $term = $related_terms[array_rand($related_terms)];
//
//      factory('chado.feature_cvterm')->create([
//        'feature_id' => $feature->feature_id,
//        'cvterm_id' => $term->cvterm_id,
//      ]);
//    }
//
//    $this->assertTrue(true);
//  }
//
//  private function set_fake_cvterm_relationships() {
//    //Add some fake cvterms with relationships
//
//    $cv = factory("chado.cv")->create();
//
//    $cvterms = factory("chado.cvterm", 20)->create([
//      'cv_id' => $cv->cv_id,
//    ]);
//
//    $cvterms_to_pop = $cvterms;
//
//    $count = 0;
//    while ($count < 10) {
//      $subject = array_pop($cvterms_to_pop)->cvterm_id;
//      $object = array_pop($cvterms_to_pop)->cvterm_id;
//
//      $cvterm_relationships = factory('chado.cvterm_relationship')->create([
//        'subject_id' => $subject,
//        'object_id' => $object,
//      ]);
//
//      $count++;
//    }
//    return $cvterms;
//
//  }
//
