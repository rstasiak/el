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


    public function load(array $command, array $data)
    {

        $table = $command['table'] ?? '';

        $hasTable = $this->hasTable($table);
        $mode = $command['mode'];

        if ($hasTable && $mode == 'replace') {

            $this->deleteTable();
            $hasTable = false;

        }

        if ( ! $hasTable) {

            $schema = SchemaHelper::generateInitialSchema(array_keys($data[0]));


            if (!empty($fields)) {

                $schema = SchemaHelper::mergeSchema($schema, $fields);

            }

            $this->createTable($table, $schema);

        }

        $this->loadToTable($data);


    }

    private function hasTable(mixed $table): bool
    {
        $sql = 'DESCRIBE TABLE ' . $table;
        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $res = $stmt->fetch(\PDO::FETCH_ASSOC);

        if (isset($res['database_name'])) {

            return true;
        }



        return false;



    }

    private function createTable(string $table, array $fields): void
    {


        $mapped = array_map(function(array $row) {

            $types = [

                'integer' => 'int',
                'string' => 'string'
            ];

            $type = $types[$row['type']];

            return $row['name'] . ' ' . $type;

        }, $fields);


        $sql = 'CREATE TABLE ' . $table . ' (' . implode(', ', $mapped) . ')';

        $this->db->exec($sql);

    }

}