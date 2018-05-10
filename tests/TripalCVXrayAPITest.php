<?php

use StatonLab\TripalTestSuite\TripalTestCase;
use StatonLab\TripalTestSuite\DBTransaction;

class TripalCVXrayAPITest extends TripalTestCase {

  use DBTransaction;

  /**
   * @group new
   */
  public function test_tripal_cv_xray_lookup_entities_for_terms_count() {

    //first load in organism and mRNA with some SO terms.

    $mrna_id = db_select("chado.cvterm", "CVT")
      ->fields('CVT', ['cvterm_id'])
      ->condition('CVT.name', 'mRNA')
      ->execute()->fetchField();


    $mrna_term = tripal_load_term_entity([
      'vocabulary' => 'SO',
      'accession' => '0000234',
    ]);
    $mrna_bundle = tripal_load_bundle_entity([
      'term_id' => $mrna_term->id,
    ]);

    $organism = factory('chado.organism')->create();
    $org_term = tripal_load_term_entity([
      'vocabulary' => 'OBI',
      'accession' => '0100026',
    ]);
    $org_bundle = tripal_load_bundle_entity(['term_id' => $org_term->id]);


    $features = factory('chado.feature', 10)->create([
      'organism_id' => $organism->organism_id,
      'type_id' => $mrna_id,
    ]);

    $query = db_select('chado.cvterm', 'cvt');
    $query->fields('cvt', ['cvterm_id']);
    $query->join('chado.cv', 'cv', 'cvt.cv_id = cv.cv_id');
    $query->join('chado.dbxref', 'dbx', 'dbx.dbxref_id = cvt.dbxref_id');
    $query->fields('dbx', ['accession']);
    $query->join('chado.db', 'db', 'db.db_id = dbx.db_id');
    $query->fields('db', ['name']);
    $query->condition('cv.name', 'sequence');

    $terms = $query->execute()->fetchAll();
    //oops we need an indexed array of these things instead in the API.
    $terms_array = [];
    foreach ($terms as $term) {
      $terms_array[] = [
        'cvterm_id' => $term->cvterm_id,
        'accession' => $term->accession,
        'vocabulary' => [
          'short_name' => $term->name,
        ],
      ];
    }

    $terms_index = [];
    $i = 0;
    foreach ($features as $feature) {
      $term = $terms[$i];

      factory('chado.feature_cvterm', 1)->create([
        'feature_id' => $feature->feature_id,
        'cvterm_id' => $term->cvterm_id,
      ]);
      $terms_index[$term->cvterm_id] = $term->name . ':' . $term->accession;
      $i++;
    }


    $this->publish('organism');
    $this->publish('feature');

    //populate the index
    tripal_cv_xray_cvterm_entity_job();


    //look up organism we published
    $query = db_select('public.chado_' . $org_bundle->name, 'CE');
    $query->fields('CE', ['entity_id']);
    $query->condition('record_id', $organism->organism_id);
    $organism_ent_id = $query->execute()->fetchField();

    $lookup = tripal_cv_xray_lookup_entities_for_terms_count($terms_array, $mrna_bundle->id, $organism_ent_id);
    $this->assertNotEmpty($lookup);
    //key is db:accession
    reset($terms_index);
    $first_key = key($terms_index);
     $accession = $terms_index[$first_key];
     $this->assertNotEquals(0, $lookup[$accession]);//this term should have been used by our entity, so it better be in the lookup
  }
}