<?php

namespace RSETL\Load\Sql;

use RSETL\Core\Collection;
use PDO;

class MysqlLoader
{
    public function __construct(protected PDO $db)
    {
    }

    public function load(array $command, Collection $collection)
    {


        $mode = $command['mode'];

        if ($mode == 'replace') {

            $this->replace($command, $collection);

        } else {

            $this->append($command, $collection);
        }

    }
//
//    private function replace(string $command, Collection $collection) {
//
//        if ($this->hasTable($tableName, $datasetName)) {
//
//            $this->deleteTable($tableName, $datasetName);
//        }
//
//        $this->append($command, $collection);
//
//    }

    private function append(array $command, Collection $collection)
    {


        $tableName = $command['table_name'] ?? '';
        $datasetName = $command['dataset_name'] ?? null;


        if (!$this->hasTable($tableName, $datasetName)) {

            $this->createTable($tableName, $collection->getFields(), $datasetName);

        }

        $this->loadData($datasetName, $tableName, $collection);

    }


    protected function createTable(string $tableName, array $fields, ?string $datasetName = null)
    {

        $sql = "CREATE TABLE " . $this->getTableName($tableName, $datasetName) . ' (' . $this->renderFields($fields) . ')';

        $stmt = $this->db->prepare($sql);
        $stmt->execute();

    }

    protected function deleteTable($tableName, $datasetName)
    {
        echo 'not implemented';
        die;
    }

    protected function hasTable(string $tableName, ?string $datasetName = null): bool
    {

        return true;
    }

    private function renderFields(array $fields): string
    {
        $sql = '';

        foreach ($fields as $key => $type) {
            $sql .= $this->renderField($key, $type) . ', ';
        }


        return substr($sql, 0, -2);
    }

    private function renderField(string $key, string $type)
    {

        $mapped = $this->mapType($type);


        return $key . ' ' . $mapped;

    }

    protected function mapType(string $type)
    {

        $types = [

            'integer' => 'INTEGER',
            'string' => 'TEXT'
        ];

        return $types[$type];


    }

    protected function getTableName(string $tableName, ?string $datasetName = null): string
    {

        return $datasetName . '.' . $tableName;

    }

    private function loadData(string $tableName, Collection $collection, ?string $datasetName = null)
    {
        $service = new CreateBulkInsertString();
        $sql = $service->create($datasetName, $tableName, $collection->getFields(), $collection->getRows());

        $stmt = $this->db->prepare($sql);
        $stmt->execute();

    }

    private function replace(array $command, Collection $collection)
    {
        $tableName = $command['table_name'];
        $datasetName = $command['dataset_name'];

        if ($this->hasTable($tableName, $datasetName)) {

            $this->deleteTable($tableName, $datasetName);
        }

        $this->append($command, $collection);

    }
}