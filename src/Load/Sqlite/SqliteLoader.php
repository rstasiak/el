<?php

namespace RStasiak\EL\Load\Sqlite;

use PDO;
use RStasiak\EL\Contract\LoaderInterface;
use RStasiak\EL\Model\Fields;
use RStasiak\EL\Service\SchemaHelper;

class SqliteLoader implements LoaderInterface
{

    public function __construct(private PDO $db)
    {
    }

    public function load(array $command, array $data): void
    {

        $table = $command['table'] ?? '';
        $fields = $command['fields'] ?? [];


        $hasTable = $this->hasTable($table);

        $mode = $command['mode'];

        if ($hasTable && $mode == 'replace') {

            $this->deleteTable($table);
            $hasTable = false;

        }

        if ( !$hasTable) {


            $schema = SchemaHelper::generateInitialSchema(array_keys($data[0]));


            if (!empty($fields)) {

                $schema = SchemaHelper::mergeSchema($schema, $fields);

            }

            $this->createTable($table, $schema);

        }

        $types = SchemaHelper::getTypes($schema);
        $this->loadToTable($table, $data, $types);

    }



    private function createTable(string $table, array $fields): void
    {

        $mapped = array_map(function(array $row) {

            $types = [

                'integer' => 'INTEGER',
                'string' => 'STRING'
            ];

            $type = $types[$row['type']];

            return $row['name'] . ' ' . $type;

        }, $fields);


        $sql = 'CREATE TABLE ' . $table . ' (' . implode(', ', $mapped) . ')';
        $this->db->exec($sql);

    }

    private function deleteTable($tableName): void
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

    private function loadToTable(string $tableName, array $data, array $types): void
    {

        $lines = array_map(function(array $row) use ($types) {

            $values = [];

            foreach($row as $key => $value) {

                if (is_null($value)) {

                    $value = '';
                }

               $type = $types[$key] ?? 'string';

                $quoteType = match ($type) {
                    default => PDO::PARAM_STR,
                    'integer' => PDO::PARAM_INT,
                };

                $values[] = $this->db->quote($value, $quoteType);
            }

            return '(' . implode(', ', $values) . ')';


        }, $data);

        $fields = array_keys($data[0]);

        $sql = 'INSERT INTO ' . $tableName . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $lines) ;

        $this->db->exec($sql);

    }
}