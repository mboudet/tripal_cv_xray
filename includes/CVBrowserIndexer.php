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
   * Keeps track of current chunk data.
   *
   * @var array
   */
  protected $data = [];

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

    foreach ($bundles as $key => $bundle) {
      $this->indexBundle($bundle);
      $this->write("Completed {$this->tally} entities");
    }

    $this->printMemoryUsage('DONE');

    $this->write("Done!");
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

    $this->write("Indexing {$bundle->label}. Total of {$total} records.");

    while ($position <= $total) {
      $this->indexChunk($bundle, $position);
      $position += $this->chunk;
    }

    if ($this->verbose) {
      print "\n";
    }
  }

  /**
   * Index a specific chunk.
   *
   * @param $bundle
   * @param $position
   *
   * @throws \Exception
   */
  public function indexChunk($bundle, $position) {
    $this->recursiveUnset($this->data);

    $this->printMemoryUsage($position);
    $entities = $this->getEntitiesChunk($bundle, $position);
    $this->loadData($entities, $bundle);
    $this->insertData();
    $this->recursiveUnset($entities);
  }

  public function recursiveUnset(&$data) {
    if(is_array($data)) {
      foreach ($data as &$value) {
        $this->recursiveUnset($value);
      }
    }

    $data = null;
    unset($data);
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
    print "Memory usage at position {$position} is {$memory}MB\r";
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
    return (int) db_select($bundle_table)
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
  public function getEntitiesChunk($bundle, $position) {
    $bundle_table = "chado_bio_data_{$bundle->bundle_id}";

    $query = db_select($bundle_table, 'CB');
    $query->fields('CB', ['entity_id', 'record_id']);
    $query->orderBy('entity_id', 'asc');
    $query->range($position, $this->chunk);

    return $query->execute()->fetchAll();
  }

  /**
   * Eager load all the data associated with the given set of entities.
   *
   * @param array $entities Must contain entity_id and record_id
   *                        from chado_bundle_N tables
   * @param object $bundle The chado bundle record
   */
  public function loadData($entities, $bundle) {
    if (empty($entities)) {
      return;
    }

    // Get the record ids as an array
    $record_ids = array_map(function ($entity) {
      return (int) $entity->record_id;
    }, $entities);

    // Get data
    $cvterms = $this->loadCVTerms($bundle->data_table, $record_ids);
    $properties = $this->loadProperties($bundle->data_table, $record_ids);
    $relatedCvterms = $this->loadRelatedCVTerms($bundle->data_table, $record_ids);
    $relatedProps = $this->loadRelatedProperties($bundle->data_table, $record_ids);

    // Index by record id
    $this->data = [];
    foreach ($entities as $key => $entity) {
      $this->data[$entity->record_id] = [
        'entity_id' => $entity->entity_id,
        'cvterms' => $cvterms[$entity->record_id] ?: [],
        'properties' => $properties[$entity->record_id] ?: [],
        'related_cvterms' => $relatedCvterms[$entity->record_id] ?: [],
        'related_props' => $relatedProps[$entity->record_id] ?: [],
      ];
    }
  }

  /**
   * Load cvterms for a given set of records in a chado table.
   *
   * @param string $table
   * @param array $record_ids Array of integers referring to
   *                            record_id in chado_bio_data_N
   *
   * @return array cvterm object indexed by record id
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
    $query->isNotNull('DB.name');
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
    $query->isNotNull('DB.name');
    $properties = $query->execute()->fetchAll();

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
  public function loadRelatedCVTerms($table, $record_ids) {
    $cvterms_by_subject = $this->loadRelatedCvtermsBy('subject_id', $table, $record_ids);
    $cvterms_by_object = $this->loadRelatedCvtermsBy('object_id', $table, $record_ids);

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
  public function loadRelatedCvtermsBy($column, $table, $record_ids) {
    $cvterm_table = "chado.{$table}_cvterm";
    $relationship_table = "chado.{$table}_relationship";
    $primary_key = $this->primaryKey($table);

    $opposite_column = $column === 'object_id' ? 'subject_id' : 'object_id';

    $query = db_select($relationship_table, 'RT');
    $query->addField('CT', $primary_key, 'record_id');
    $query->fields('CVT', ['cvterm_id']);
    $query->fields('DB', ['name']);
    $query->fields('DBX', ['accession']);
    $query->fields('RT', [$column]);
    $query->join($cvterm_table, 'CT', 'RT.' . $opposite_column . ' = CT.' . $primary_key);
    $query->join('chado.cvterm', 'CVT', 'CT.cvterm_id = CVT.cvterm_id');
    $query->join("chado.dbxref", "DBX", "CVT.dbxref_id = DBX.dbxref_id");
    $query->join("chado.db", "DB", "DBX.db_id = DB.db_id");
    $query->condition('RT.' . $column, $record_ids, 'IN');
    $query->isNotNull('DB.name');
    return $query->execute()->fetchAll();
  }

  /**
   * Get all related properties from _relationship tables.
   *
   * @param $table
   * @param $record_ids
   *
   * @return array
   */
  public function loadRelatedProperties($table, $record_ids) {
    $properties_by_subject = $this->loadRelatedPropertiesBy('subject_id', $table, $record_ids);
    $properties_by_object = $this->loadRelatedPropertiesBy('object_id', $table, $record_ids);

    $added = [];
    $data = [];
    foreach ($properties_by_object as $property) {
      // avoid inserting duplicate cvterm ids
      if (!isset($added[$property->object_id][$property->cvterm_id])) {
        $added[$property->object_id][$property->cvterm_id] = TRUE;
        $data[$property->object_id][] = $property;
      }
    }

    foreach ($properties_by_subject as $property) {
      // avoid inserting duplicate cvterm ids
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
  public function loadRelatedPropertiesBy($column, $table, $record_ids) {
    $prop_table = "chado.{$table}prop";
    $relationship_table = "chado.{$table}_relationship";
    $primary_key = $this->primaryKey($table);

    $opposite_column = $column === 'object_id' ? 'subject_id' : 'object_id';

    $query = db_select($relationship_table, 'RT');
    $query->addField('PT', $primary_key, 'record_id');
    $query->fields('CVT', ['cvterm_id']);
    $query->fields('DB', ['name']);
    $query->fields('DBX', ['accession']);
    $query->fields('RT', [$column]);
    $query->join($prop_table, 'PT', 'RT.' . $opposite_column . ' = PT.' . $primary_key);
    $query->join('chado.cvterm', 'CVT', 'PT.type_id = CVT.cvterm_id');
    $query->join("chado.dbxref", "DBX", "CVT.dbxref_id = DBX.dbxref_id");
    $query->join("chado.db", "DB", "DBX.db_id = DB.db_id");
    $query->condition('RT.' . $column, $record_ids, 'IN');
    $query->isNotNull('DB.name');

    return $query->execute()->fetchAll();
  }

  /**
   * Insert data into the index table.
   *
   *
   * @see \CVBrowserIndexer::loadData()
   * @throws \Exception
   * @return \DatabaseStatementInterface|int
   */
  public function insertData() {
    $query = db_insert('tripal_cvterm_entity_linker')->fields([
      'entity_id',
      'cvterm_id',
      'database',
      'accession',
    ]);

    foreach ($this->data as $record_id => $record) {
      $entity_id = $record['entity_id'];

      foreach ($record['cvterms'] as $cvterm) {
        $query->values($this->extractCvtermForInsertion($cvterm, $entity_id));
      }

      foreach ($record['related_cvterms'] as $cvterm) {
        $query->values($this->extractCvtermForInsertion($cvterm, $entity_id));
      }

      foreach ($record['properties'] as $property) {
        $query->values($this->extractCvtermForInsertion($property, $entity_id));
      }

      foreach ($record['related_props'] as $property) {
        $query->values($this->extractCvtermForInsertion($property, $entity_id));
      }
    }

    return $query->execute();
  }

  /**
   * Extracts the needed data for insertion into the linker table.
   *
   * @param $data
   * @param $entity_id
   *
   * @return array
   */
  public function extractCvtermForInsertion($data, $entity_id) {
    return [
      'entity_id' => $entity_id,
      'cvterm_id' => $data->cvterm_id,
      'database' => $data->name,
      'accession' => $data->accession,
    ];
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

  /**
   * Write a line to STDOUT if verbose mode is enabled.
   *
   * @param $line
   */
  public function write($line) {
    if ($this->verbose) {
      print "$line\n";
    }
  }

  /**
   * Runs the indexer in verbose mode.
   */
  public static function run() {
    $memory = number_format(memory_get_usage() / 1024 / 1024);
    print "Memory usage at START is {$memory}MB\n";

    $indexer = new static();
    $indexer->clearIndexTable();
    $indexer->setChunkSize(1000);
    $indexer->index(TRUE);
    $indexer = null;
    unset($indexer);

    $memory = number_format(memory_get_usage() / 1024 / 1024);
    print "Memory usage at END is {$memory}MB\n";
  }
}
