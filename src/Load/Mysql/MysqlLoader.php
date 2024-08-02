<?php

namespace RStasiak\EL\Load\Mysql;

use PDO;
use RStasiak\EL\Service\SchemaHelper;

class MysqlLoader
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

        $schema = SchemaHelper::generateInitialSchema(array_keys($data[0]));

        if (!empty($fields)) {

            $schema = SchemaHelper::mergeSchema($schema, $fields);

        }

        if (!$hasTable) {


            $this->createTable($table, $schema);

        }

        $types = SchemaHelper::getTypes($schema);
        $this->loadToTable($table, $data, $types);

    }



    private function createTable(string $table, array $fields): void
    {


        $mapped = array_map(function (array $row) {

            $types = [

                'integer' => 'INT',
                'string' => 'TEXT'
            ];

            $type = $types[$row['type']];

            return $row['name'] . ' ' . $type;

        }, $fields);


        $sql = 'CREATE TABLE ' . $table . ' (' . implode(', ', $mapped) . ')';

        $this->db->exec($sql);

    }

    private function deleteTable($table): void
    {
        $sql = "DROP TABLE IF EXISTS " . $table;
        $this->db->exec($sql);

    }


    private function hasTable(string $table): bool
    {


        $sql = "SHOW TABLES LIKE '". $table . "'";
        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $res = $stmt->fetch(\PDO::FETCH_ASSOC);

        if ($res === false) {

            return false;
        }

        return true;








    }

    private function loadToTable(string $table, array $data, array $types): void
    {

        $lines = array_map(function (array $row) use ($types) {

            $values = [];

            foreach ($row as $key => $value) {

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

        $sql = 'INSERT INTO ' . $table . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $lines);

        $this->db->exec($sql);

    }
}