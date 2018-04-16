<?php

class CVBrowserIndexer {

  /**
   * Chunk size.
   *
   * @var int
   */
  protected $chunk = 100;

  /**
   * Supported chado tables.
   *
   * @var array
   */
  protected $supportedTables = ['feature'];

  /**
   * Primary keys cache.
   *
   * @var array
   */
  protected static $primaryKeys = [];

  /**
   * Verbose output or silent.
   *
   * @var bool
   */
  protected $verbose;

  /**
   * Keep the tally.
   *
   * @var int
   */
  protected $tally = 0;

  /**
   * Start the indexing job.
   *
   * @param bool $print_info whether to print memory and progress info.
   *
   * @throws \Exception
   */
  public function index($print_info = FALSE) {
    $this->verbose = $print_info;

    $bundles = $this->bundles();

    foreach ($bundles as $bundle) {
      $this->indexBundle($bundle);

      if ($this->verbose) {
        print "Completed {$this->tally} entities\n";
      }
    }
  }

  /**
   * Chunks and indexes data of a specific bundle.
   *
   * @param $bundle
   *
   * @throws \Exception
   */
  public function indexBundle($bundle) {
    $total = $this->bundleTotal($bundle);
    $this->tally += $total;
    $position = 0;

    if ($this->verbose) {
      print "Indexing {$bundle->label}. Total of {$total} records.\n";
    }

    while ($position <= $total) {
      $this->printMemoryUsage($position);
      $entities = $this->getEntitiesChunk($bundle, $position);
      $this->printMemoryUsage($position - $this->chunk);
      $data = $this->loadData($entities, $bundle);
      $this->printMemoryUsage($position - $this->chunk);
      $this->insertData($data);
      unset($data);
      unset($entities);
      $this->printMemoryUsage($position - $this->chunk);
      ob_flush();
    }
  }

  /**
   * Print memory usage if verbose option is specified.
   *
   * @param int $position Position of chunk.
   */
  protected function printMemoryUsage($position) {
    if (!$this->verbose) {
      return;
    }

    $memory = number_format(memory_get_usage() / 1024 / 1024);
    print "Memory usage at position {$position} is {$memory}MB\n";
  }

  /**
   * Get the total number of entities in a bundle.
   *
   * @param $bundle
   *
   * @return int
   */
  public function bundleTotal($bundle) {
    $bundle_table = "chado_bio_data_{$bundle->bundle_id}";
    return (int) db_select($bundle_table, 'BT')
      ->countQuery()
      ->execute()
      ->fetchField();
  }

  /**
   * Get a chunk of entities.
   *
   * @param object $bundle The bundle record.
   * @param int $position The starting position (this gets auto incremented to
   *                        the next position)
   *
   * @return array Chunk of entities. Returns empty array
   */
  public function getEntitiesChunk($bundle, &$position) {
    $bundle_table = "chado_bio_data_{$bundle->bundle_id}";
    $query = db_select($bundle_table, 'CB');
    $query->fields('CB', ['entity_id', 'record_id']);
    $query->range($position, $this->chunk);

    $position += $this->chunk;

    return $query->execute()->fetchAll();
  }

  /**
   * Eager load all the data associated with the given set of entities.
   *
   * @param array $entities Must contain entity_id and record_id
   *                        from chado_bundle_N tables
   * @param object $bundle The chado bundle record
   *
   * @return array
   */
  public function loadData($entities, $bundle) {
    $data = [];

    if (empty($entities)) {
      return [];
    }

    // Get the record ids as an array
    $record_ids = array_map(function ($entity) {
      return (int) $entity->record_id;
    }, $entities);

    // Get data
    $cvterms = $this->loadCVTerms($bundle->data_table, $record_ids);
    $properties = $this->loadProperties($bundle->data_table, $record_ids);

    // Index by record id
    foreach ($entities as $entity) {
      $data[$entity->record_id] = [
        'entity' => $entity,
        'cvterms' => $cvterms[$entity->record_id] ?: [],
        'properties' => $properties[$entity->record_id] ?: [],
      ];
    }

    return $data;
  }

  /**
   * Load cvterms for a given set of records in a chado table.
   *
   * @param string $table
   * @param array $record_ids Array of integers referring to
   *                            record_id in chado_bio_data_N
   *
   * @return array Indexed by record id
   */
  public function loadCVTerms($table, $record_ids) {
    $cvterm_table = "chado.{$table}_cvterm";
    $primary_key = $this->primaryKey($table);

    $query = db_select($cvterm_table, 'CT');
    $query->addField('CT', $primary_key, 'record_id');
    $query->fields('CVT', ['cvterm_id']);
    $query->fields('DB', ['name']);
    $query->fields('DBX', ['accession']);
    $query->join('chado.cvterm', 'CVT', 'CT.cvterm_id = CVT.cvterm_id');
    $query->join("chado.dbxref", "DBX", "CVT.dbxref_id = DBX.dbxref_id");
    $query->join("chado.db", "DB", "DBX.db_id = DB.db_id");
    $query->condition($primary_key, $record_ids, 'IN');
    $cvterms = $query->execute()->fetchAll();

    $data = [];
    foreach ($cvterms as $cvterm) {
      $data[$cvterm->record_id][] = $cvterm;
    }

    return $data;
  }

  /**
   * Get properties for a given set of records.
   *
   * @param string $table chado table name
   * @param array $record_ids Record ids
   *
   * @return array
   */
  public function loadProperties($table, $record_ids) {
    $props_table = "chado.{$table}prop";
    $primary_key = $this->primaryKey($table);

    $query = db_select($props_table, 'CT');
    $query->addField('CT', $primary_key, 'record_id');
    $query->fields('CVT', ['cvterm_id']);
    $query->fields('DB', ['name']);
    $query->fields('DBX', ['accession']);
    $query->join('chado.cvterm', 'CVT', 'CT.type_id = CVT.cvterm_id');
    $query->join("chado.dbxref", "DBX", "CVT.dbxref_id = DBX.dbxref_id");
    $query->join("chado.db", "DB", "DBX.db_id = DB.db_id");
    $query->condition($primary_key, $record_ids, 'IN');
    $properties = $query->execute()->fetchAll();

    $data = [];
    foreach ($properties as $property) {
      $data[$property->record_id][] = $property;
    }

    return $data;
  }

  /**
   * Insert data into the index table.
   *
   * @param array $data Array structured as returned in loadData
   *
   * @see \CVBrowserIndexer::loadData()
   * @throws \Exception
   * @return \DatabaseStatementInterface|int
   */
  public function insertData($data) {
    $query = db_insert('tripal_cvterm_entity_linker')->fields([
      'entity_id',
      'cvterm_id',
      'database',
      'accession',
    ]);

    foreach ($data as $record_id => $record) {
      $entity = $record['entity'];
      $cvterms = $record['cvterms'];
      $properties = $record['properties'];

      foreach ($cvterms as $cvterm) {
        $query->values([
          'entity_id' => $entity->entity_id,
          'cvterm_id' => $cvterm->cvterm_id,
          'database' => $cvterm->name,
          'accession' => $cvterm->accession,
        ]);
      }

      foreach ($properties as $property) {
        $query->values([
          'entity_id' => $entity->entity_id,
          'cvterm_id' => $property->cvterm_id,
          'database' => $property->name,
          'accession' => $property->accession,
        ]);
      }
    }

    return $query->execute();
  }

  /**
   * Get and cache primary key of a chado table.
   *
   * @param string $table table name such as "feature".
   *
   * @return mixed|string
   */
  public function primaryKey($table) {
    if (isset(static::$primaryKeys[$table])) {
      return static::$primaryKeys[$table];
    }

    $schema = chado_get_schema($table);
    if (isset($schema['primary key']) && !empty($schema['primary key'])) {
      $key = $schema['primary key'][0];
      static::$primaryKeys[$table] = $key;

      return $key;
    }

    $key = "{$table}_id";
    static::$primaryKeys[$table] = $key;
    return $key;
  }

  /**
   * Get the bundles we are interested in.
   *
   * @return mixed
   */
  public function bundles() {
    $query = db_select('chado_bundle', 'CB');
    $query->fields('CB', ['bundle_id', 'data_table']);
    $query->fields('TB', ['label']);
    $query->join('tripal_bundle', 'TB', 'TB.id = CB.bundle_id');
    $query->condition('data_table', $this->supportedTables, 'IN');

    return $query->execute()->fetchAll();
  }

  /**
   * Set the chunk size.
   *
   * @param int $size Number of elements per chunk.
   */
  public function setChunkSize($size) {
    $this->chunk = $size;
  }

  /**
   * Truncate the tripal_cvterm_entity_linker table.
   *
   * @return \DatabaseStatementInterface
   */
  public function clearIndexTable() {
    return db_truncate('tripal_cvterm_entity_linker')->execute();
  }
}
