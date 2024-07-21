<?php

namespace RStasiak\EL\Load\Oxla;

use PDO;
use RStasiak\EL\Contract\LoaderInterface;
use RStasiak\EL\Service\SchemaHelper;

class OxlaLoader implements LoaderInterface
{

    public function __construct(private PDO $db)
    {
    }

    public function load(array $command, array $data): void
    {

        $dataset = $command['dataset'] ?? '';
        $table = $command['table'] ?? '';
        $fields = $command['fields'] ?? [];


        $hasTable = $this->hasTable($dataset, $table);

        $mode = $command['mode'];

        if ($hasTable && $mode == 'replace') {

            $this->deleteTable($dataset, $table);
            $hasTable = false;

        }

        $schema = SchemaHelper::generateInitialSchema(array_keys($data[0]));


        if (!empty($fields)) {

            $schema = SchemaHelper::mergeSchema($schema, $fields);

        }

        if (!$hasTable) {


            $this->createTable($dataset, $table, $schema);

        }

        $types = SchemaHelper::getTypes($schema);
        $this->loadToTable($dataset, $table, $data, $types);

    }



    private function createTable(string $dataset, string $table, array $fields): void
    {

        $this->db->exec('CREATE SCHEMA IF NOT EXISTS ' . $dataset);

        $mapped = array_map(function (array $row) {

            $types = [

                'integer' => 'integer',
                'string' => 'string'
            ];

            $type = $types[$row['type']];

            return $row['name'] . ' ' . $type;

        }, $fields);


        $sql = 'CREATE TABLE ' . $dataset . '.' . $table . ' (' . implode(', ', $mapped) . ')';
        $this->db->exec($sql);

    }

    private function deleteTable($dataset, $table): void
    {
        $sql = "DROP TABLE IF EXISTS " . $dataset . '.' . $table;
        $this->db->exec($sql);

    }


    private function hasTable(string $dataset, string $table): bool
    {


        $sql = 'DESCRIBE TABLE ' . $dataset . '.' . $table;
        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $res = $stmt->fetch(\PDO::FETCH_ASSOC);

        return !($res === false);






    }

    private function loadToTable(string $dataset, string $table, array $data, array $types): void
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

        $sql = 'INSERT INTO ' . $dataset . '.' . $table . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $lines);


        $this->db->exec($sql);

    }
}