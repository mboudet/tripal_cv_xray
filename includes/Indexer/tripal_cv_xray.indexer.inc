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

    $indexer = new CVBrowserChunkIndexer($entities, $bundle);
    $indexer->index();
    $position += $chunk;

    // Clean up memory
    $indexer = NULL;
    unset($indexer);
  }
  print "Memory usage at position {$position} is {$memory}MB\r";
  print "\n";
}
