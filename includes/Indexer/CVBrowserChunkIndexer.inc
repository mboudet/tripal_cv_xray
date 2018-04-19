<?php

class CVBrowserChunkIndexer {

  /**
   * Entities chunk.
   *
   * @var array
   */
  protected $entities;

  /**
   * The bundle object.
   *
   * @var object
   */
  protected $bundle;

  /**
   * Holds a cache of primary keys.
   *
   * @var array
   */
  protected static $primaryKeys = [];

  /**
   * CVBrowserChunkIndexer constructor.
   *
   * @param array $entities
   * @param object $bundle
   */
  public function __construct(array $entities, $bundle) {
    $this->entities = $entities;
    $this->bundle = $bundle;
  }

  /**
   * Start indexing cvterms for the given chunk.
   *
   * @param bool $verbose
   *
   * @throws Exception
   */
  public function index() {
    $data = $this->loadData();
    $this->insertData($data);
  }

  /**
   * Eager load all the data associated with the given set of entities.
   *
   * @param array $entities Must contain entity_id and record_id
   *                        from chado_bundle_N tables
   *
   * @return array
   */
  public function loadData() {
    // Get the record ids as an array
    $record_ids = [];
    $entities = [];
    foreach ($this->entities as $entity) {
      $record_ids[] = $entity->record_id;
      $entities[] = $entity;
    }

    if (empty($record_ids)) {
      return [];
    }

    // Get data
    $cvterms = $this->loadCVTerms($this->bundle->data_table, $record_ids);
    $properties = $this->loadProperties($this->bundle->data_table, $record_ids);
    $relatedCvterms = $this->loadRelatedCVTerms($this->bundle->data_table, $record_ids);
    $relatedProps = $this->loadRelatedProperties($this->bundle->data_table, $record_ids);

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

    return $data;
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
  public function loadRelatedProperties($table, $record_ids) {
    $properties_by_subject = $this->loadRelatedPropertiesBy('subject_id', $table, $record_ids);
    $properties_by_object = $this->loadRelatedPropertiesBy('object_id', $table, $record_ids);

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
  public function loadRelatedPropertiesBy($column, $table, $record_ids) {
    $prop_table = "chado.{$table}prop";
    $relationship_table = "chado.{$table}_relationship";
    $primary_key = $this->primaryKey($table);
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

  /**
   * Insert data into the index table.
   *
   *
   * @see \CVBrowserIndexer::loadData()
   * @throws \Exception
   * @return void
   */
  public function insertData(&$data) {
    $query = db_insert('tripal_cvterm_entity_linker')->fields([
      'entity_id',
      'cvterm_id',
      'database',
      'accession',
    ]);

    foreach ($data as $record_id => $record) {
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

    $query->execute();
    unset($query);
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
   * Clean up the object when done with it
   */
  public function __destruct() {
    $this->entities = NULL;
    $this->bundle = NULL;
  }
}
