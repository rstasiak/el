<?php

namespace RSETL\Load\Sqlite;

use RSETL\Core\Collection;
use RSETL\Contract\LoaderInterface;
use RSETL\Service\BulkInsertService;
use PDO;

class SqliteLoader implements LoaderInterface
{

    public function __construct(private PDO $db)
    {
    }

    public function load(array $command, Collection $collection)
    {
        $allowed = ['append', 'replace'];
        $mode = $command['mode'];

        if (!in_array($mode, $allowed))
        {
            throw new \Exception('bad mode');
        }

        if ($mode == 'replace') {

            $this->replace($command, $collection);

        } else {

            $this->append($command, $collection);
        }


    }

    private function replace(array $command, Collection $collection)
    {
        $tableName = $command['table'];

        if ($this->hasTable($tableName)) {

            $this->deleteTable($tableName);
        }

        $this->append($command, $collection);

    }

    private function append(array $command, Collection $collection)
    {

        $tableName = $command['table'];

        if (!$this->hasTable($tableName)) {

            $this->createTable($tableName, $collection->getFields());

        }

        $this->loadData($tableName, $collection);

    }

    private function createTable(string $tableName, array $fields) {

        $sql = 'CREATE TABLE ' . $tableName . '(' . $this->renderCreateFields($fields) . ')';
        $this->db->exec($sql);

    }

    private function deleteTable($tableName)
    {
        $sql = "DROP TABLE IF EXISTS " . $tableName;
        $this->db->exec($sql);

    }

    private function hasTable(string $tableName): bool
    {
        $sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=" . $this->db->quote($tableName);
        $result = $this->db->query($sql);
        return $result !== false && $result->fetch() !== false;

    }

    private function loadData(string $tableName, Collection $collection)
    {
        $service = new BulkInsertService();

        $sql = 'INSERT INTO ' . $tableName . ' (' . $this->renderInsertFields($collection->getFields()) . ') VALUES ' ;
        $sql .= $service->create($collection);

        $this->db->exec($sql);

    }

    private function renderCreateFields(array $fields): string
    {


        $types = [

            'string' => 'TEXT',
            'integer' => 'INTEGER',
            'float' => 'REAL'
        ];

        $string = '';

        foreach($fields as $key => $type) {

            $string .= $key . ' ' . $types[$type] . ', ';

        }

        $string = substr($string, 0, -2);

        return $string;

    }

    private function renderInsertFields(array $fields): string
    {


        $string = '';

        foreach($fields as $key => $type) {

            $string .= $key . ', ';

        }

        return substr($string, 0, -2);

    }

}