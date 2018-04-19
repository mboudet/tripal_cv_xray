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

    foreach ($bundles as $bundle) {
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
      gc_enable();
      $this->printMemoryUsage($position);
      $entities = $this->getEntitiesChunk($bundle, $position);
      $indexer = new CVBrowserChunkIndexer($entities, $bundle);
      $indexer->index();
      $position += $this->chunk;

      // Clean up memory
      $indexer = null;
      unset($indexer);
      gc_collect_cycles();
      gc_disable();
    }

    if ($this->verbose) {
      print "\n";
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
   */
  public function getEntitiesChunk($bundle, $position) {
    $bundle_table = "chado_bio_data_{$bundle->bundle_id}";

    $query = db_select($bundle_table, 'CB');
    $query->fields('CB', ['entity_id', 'record_id']);
    $query->orderBy('entity_id', 'asc');
    $query->range($position, $this->chunk);

    return $query->execute()->fetchAll();
  }

  protected function error($line) {
    print "\033[31m$line\033[0m\n";
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
    $indexer = new static();
    $indexer->clearIndexTable();
    $indexer->setChunkSize(1000);
    $indexer->index(TRUE);
  }
}
