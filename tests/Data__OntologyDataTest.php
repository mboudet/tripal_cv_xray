<?php

use StatonLab\TripalTestSuite\TripalTestCase;
use StatonLab\TripalTestSuite\DBTransaction;

require_once(__DIR__ . '/../includes/TripalFields/data__ontology_data/data__ontology_data.inc');


class Data__OntologyDataTest extends TripalTestCase {

  use DBTransaction;

  /**
   * @group working
   */
  public function test_class_loads() {

    //create and publish an organism

    $organism = factory('chado.organism')->create(['genus' => 'retrieve_me']);

    $org_term = tripal_load_term_entity([
      'vocabulary' => 'OBI',
      'accession' => '0100026',
    ]);
    $org_bundle = tripal_load_bundle_entity(['term_id' => $org_term->id]);

    tripal_chado_publish_records(['bundle_name' => $org_bundle->name]);

    $mrna_term = tripal_load_term_entity([
      'vocabulary' => 'SO',
      'accession' => '0000234'
    ]);
    $mrna_bundle = tripal_load_bundle_entity([
      'term_id' =>$mrna_term->id
    ]);
    //these dont work in this context!!!!
    //    $fif = field_info_field('data__ontology_data');
    //    $fei = field_info_instance('TripalEntity', 'data__ontology_data', $org_bundle->name);

    $field_name = 'data__ontology_data';
    $fif = [];
    $fif['field_name'] == $field_name;

    $fei = [];
    $fei['settings']['container'] = [
      'ontology' => 'GO',
      'target_bundle' => $mrna_bundle->id];

    //get entity id
    $query = db_select('public.chado_' . $org_bundle->name, 'cbdx');
    $query->fields('cbdx', ['entity_id']);
    $query->condition('record_id', $organism->organism_id, '=');
    $entity_id = $query->execute()->fetchField();

    $entity = tripal_load_entity('TripalEntity', [$entity_id]);

    $entity = $entity[$entity_id];

    $obj = new data__ontology_data($fif, $fei);
    $obj->load($entity);

    //but field hasnt attached to entity.... and how could it?  we dont pass by ref.

  }


}