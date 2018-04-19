<?php
function tripal_cv_xray_get_bundles() {
  $query = db_query('SELECT CB.bundle_id, CB.data_table, TB.label
              FROM {chado_bundle} CB
              INNER JOIN {tripal_bundle} TB ON TB.id = CB.bundle_id
              WHERE data_table IN (:supported)', [
    ':supported' => [
      'feature',
    ],
  ]);

  return $query->fetchAll();
}

function tripal_cv_xray_index() {
  db_truncate('tripal_cvterm_entity_linker')->execute();

  $bundles = tripal_cv_xray_get_bundles();
  foreach ($bundles as $bundle) {
    tripal_cv_xray_get_index_bundle($bundle);
  }

  print "Done!\n";
}

function tripal_cv_xray_get_index_bundle($bundle) {
  $bundle_table = "chado_bio_data_{$bundle->bundle_id}";
  $total = (int) db_query("SELECT COUNT(*) FROM {{$bundle_table}}")->fetchField();
  $position = 0;
  $chunk = 1000;

  while ($position <= $total) {
    $memory = number_format(memory_get_usage() / 1024 / 1024);
    print "Memory usage at position {$position} is {$memory}MB\r";

    $query = db_query("SELECT CB.entity_id, CB.record_id
                        FROM {{$bundle_table}} CB
                        ORDER BY entity_id ASC
                        OFFSET :offset
                        LIMIT :limit", [
      ':offset' => $position,
      ':limit' => $chunk,
    ]);
    $entities = $query->fetchAll();

    $record_ids = array_map(function ($entity) {
      return $entity->record_id;
    }, $entities);

    // Get data
    $cvterms = tripal_cv_xray_loadCVTerms($bundle->data_table, $record_ids);
    $properties = tripal_cv_xray_loadProperties($bundle->data_table, $record_ids);
    $relatedCvterms = tripal_cv_xray_loadRelatedCVTerms($bundle->data_table, $record_ids);
    $relatedProps = tripal_cv_xray_loadRelatedProperties($bundle->data_table, $record_ids);

    // Index by record id
    $data = [];
    foreach ($entities as $key => $entity) {
      $data[$entity->record_id] = [
        'entity_id' => $entity->entity_id,
        'cvterms' => $cvterms[$entity->record_id] ?: [],
        'properties' => $properties[$entity->record_id] ?: [],
        'related_cvterms' => $relatedCvterms[$entity->record_id] ?: [],
        'related_props' => $relatedProps[$entity->record_id] ?: [],
      ];
    }

    $position += $chunk;

    // Clean up memory
    $indexer = NULL;
    unset($indexer);
  }
  print "Memory usage at position {$position} is {$memory}MB\r";
  print "\n";
}

function tripal_cv_xray_loadCVTerms($table, $record_ids) {
  $cvterm_table = "chado.{$table}_cvterm";
  $primary_key = tripal_cv_xray_primaryKey($table);

  $query = db_query("SELECT CT.{$primary_key} as record_id,
                              CVT.cvterm_id,
                              DB.name,
                              DBX.accession
                        FROM {$cvterm_table} CT
                        INNER JOIN chado.cvterm CVT ON CT.cvterm_id = CVT.cvterm_id
                        INNER JOIN chado.dbxref DBX ON CVT.dbxref_id = DBX.dbxref_id
                        INNER JOIN chado.db DB ON DBX.db_id = DB.db_id
                        WHERE {$primary_key} IN (:ids)
                              AND DB.name IS NOT NULL", [':ids' => $record_ids]);
  $cvterms = $query->fetchAll();

  $data = [];
  foreach ($cvterms as $cvterm) {
    $data[$cvterm->record_id][] = $cvterm;
  }

  return $data;
}

/**
 * Get and cache primary key of a chado table.
 *
 * @param string $table table name such as "feature".
 *
 * @return mixed|string
 */
function tripal_cv_xray_primaryKey($table) {
  static $primaryKeys;

  if (isset($primaryKeys[$table])) {
    return $primaryKeys[$table];
  }

  $schema = chado_get_schema($table);
  if (isset($schema['primary key']) && !empty($schema['primary key'])) {
    $key = $schema['primary key'][0];
    $primaryKeys[$table] = $key;

    return $key;
  }

  $key = "{$table}_id";
  $primaryKeys[$table] = $key;
  return $key;
}

/**
 * Get properties for a given set of records.
 *
 * @param string $table chado table name
 * @param array $record_ids Record ids
 *
 * @return array
 */
function tripal_cv_xray_loadProperties($table, $record_ids) {
  $props_table = "chado.{$table}prop";
  $primary_key = tripal_cv_xray_primaryKey($table);

  $query = db_query("SELECT PT.{$primary_key} as record_id,
                              CVT.cvterm_id,
                              DB.name,
                              DBX.accession
                        FROM {$props_table} PT
                        INNER JOIN chado.cvterm CVT ON PT.type_id = CVT.cvterm_id
                        INNER JOIN chado.dbxref DBX ON CVT.dbxref_id = DBX.dbxref_id
                        INNER JOIN chado.db DB ON DBX.db_id = DB.db_id
                        WHERE {$primary_key} IN (:ids)
                              AND DB.name IS NOT NULL", [':ids' => $record_ids]);
  $properties = $query->fetchAll();

  $data = [];
  foreach ($properties as $property) {
    $data[$property->record_id][] = $property;
  }

  return $data;
}

/**
 * Loads all related cvterms from _relationship table.
 *
 * @param $table
 * @param $record_ids
 *
 * @return array
 */
function tripal_cv_xray_loadRelatedCVTerms($table, $record_ids) {
  $cvterms_by_subject = tripal_cv_xray_loadRelatedCvtermsBy('subject_id', $table, $record_ids);
  $cvterms_by_object = tripal_cv_xray_loadRelatedCvtermsBy('object_id', $table, $record_ids);

  $added = [];
  $data = [];
  foreach ($cvterms_by_object as $cvterm) {
    // avoid inserting duplicate cvterm ids
    if (!isset($added[$cvterm->object_id][$cvterm->cvterm_id])) {
      $added[$cvterm->object_id][$cvterm->cvterm_id] = TRUE;
      $data[$cvterm->object_id][] = $cvterm;
    }
  }

  foreach ($cvterms_by_subject as $cvterm) {
    // avoid inserting duplicate cvterm ids
    if (!isset($added[$cvterm->subject_id][$cvterm->cvterm_id])) {
      $added[$cvterm->subject_id][$cvterm->cvterm_id] = TRUE;
      $data[$cvterm->subject_id][] = $cvterm;
    }
  }

  return $data;
}

/**
 * Loads all related cvterms by subject or object.
 *
 * @param $column
 * @param $table
 * @param $record_ids
 *
 * @return mixed
 */
function tripal_cv_xray_loadRelatedCvtermsBy($column, $table, $record_ids) {
  $cvterm_table = "chado.{$table}_cvterm";
  $relationship_table = "chado.{$table}_relationship";
  $primary_key = tripal_cv_xray_primaryKey($table);
  $opposite_column = $column === 'object_id' ? 'subject_id' : 'object_id';

  $sql = "SELECT CT.$primary_key as record_id,
                               CVT.cvterm_id,
                               DB.name,
                               DBX.accession,
                               RT.$column
                        FROM $relationship_table RT
                        INNER JOIN $cvterm_table CT ON RT.$opposite_column = CT.$primary_key
                        INNER JOIN chado.cvterm CVT ON CT.cvterm_id = CVT.cvterm_id
                        INNER JOIN chado.dbxref DBX ON CVT.dbxref_id = DBX.dbxref_id
                        INNER JOIN chado.db DB ON DBX.db_id = DB.db_id
                        WHERE RT.$opposite_column IN (:ids)
                              AND DB.name IS NOT NULL";
  $query = db_query($sql, [':ids' => $record_ids]);

  return $query->fetchAll();
}

/**
 * Get all related properties from _relationship tables.
 *
 * @param $table
 * @param $record_ids
 *
 * @return array
 */
function tripal_cv_xray_loadRelatedProperties($table, $record_ids) {
  $properties_by_subject = tripal_cv_xray_loadRelatedPropertiesBy('subject_id', $table, $record_ids);
  $properties_by_object = tripal_cv_xray_loadRelatedPropertiesBy('object_id', $table, $record_ids);

  $added = [];
  $data = [];
  foreach ($properties_by_object as $property) {
    // Avoid inserting duplicate cvterm ids
    if (!isset($added[$property->object_id][$property->cvterm_id])) {
      $added[$property->object_id][$property->cvterm_id] = TRUE;
      $data[$property->object_id][] = $property;
    }
  }

  foreach ($properties_by_subject as $property) {
    // Avoid inserting duplicate cvterm ids
    if (!isset($added[$property->subject_id][$property->cvterm_id])) {
      $added[$property->subject_id][$property->cvterm_id] = TRUE;
      $data[$property->subject_id][] = $property;
    }
  }

  return $data;
}

/**
 * Get related properties by specifying subject or object
 *
 * @param $column
 * @param $table
 * @param $record_ids
 *
 * @return mixed
 */
function tripal_cv_xray_loadRelatedPropertiesBy($column, $table, $record_ids) {
  $prop_table = "chado.{$table}prop";
  $relationship_table = "chado.{$table}_relationship";
  $primary_key = tripal_cv_xray_primaryKey($table);
  $opposite_column = $column === 'object_id' ? 'subject_id' : 'object_id';

  $query = db_query("SELECT PT.{$primary_key} as record_id,
                              CVT.cvterm_id,
                              DB.name,
                              DBX.accession,
                              RT.{$opposite_column}
                        FROM {$relationship_table} RT
                        INNER JOIN {$prop_table} PT ON RT.{$opposite_column} = PT.{$primary_key}
                        INNER JOIN chado.cvterm CVT ON PT.type_id = CVT.cvterm_id
                        INNER JOIN chado.dbxref DBX ON CVT.dbxref_id = DBX.dbxref_id
                        INNER JOIN chado.db DB ON DBX.db_id = DB.db_id
                        WHERE RT.{$column} IN (:ids)
                              AND DB.name IS NOT NULL", [':ids' => $record_ids]);

  return $query->fetchAll();
}
