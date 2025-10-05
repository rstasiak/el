<?php

namespace RStasiak\EL\Load\Mysql;

use PDO;
use RStasiak\EL\Service\SchemaHelper;
use InvalidArgumentException;

class MysqlLoader
{
    public function __construct(private PDO $db)
    {
    }

    private function isValidIdentifier(string $name): bool
    {
        // MySQL identifiers: alphanumeric + underscore, max 64 chars
        return preg_match('/^[a-zA-Z0-9_]{1,64}$/', $name) === 1;
    }

    private function escapeIdentifier(string $identifier): string
    {
        if (!$this->isValidIdentifier($identifier)) {
            throw new InvalidArgumentException("Invalid identifier: $identifier");
        }
        return "`$identifier`";
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
        $types = [
            'integer' => 'INT',
            'string' => 'TEXT'
        ];

        $mapped = array_map(function (array $row) use ($types) {
            $type = $types[$row['type']] ?? 'TEXT';
            $columnName = $this->escapeIdentifier($row['name']);
            return "$columnName $type";
        }, $fields);

        $tableName = $this->escapeIdentifier($table);
        $sql = "CREATE TABLE $tableName (" . implode(', ', $mapped) . ')';

        $this->db->exec($sql);
    }

    private function deleteTable(string $table): void
    {
        $tableName = $this->escapeIdentifier($table);
        $sql = "DROP TABLE IF EXISTS $tableName";
        $this->db->exec($sql);
    }


    private function hasTable(string $table): bool
    {
        $stmt = $this->db->prepare("SHOW TABLES LIKE ?");
        $stmt->execute([$table]);
        return $stmt->fetch(PDO::FETCH_ASSOC) !== false;
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
        $escapedFields = array_map(fn($field) => $this->escapeIdentifier($field), $fields);

        $tableName = $this->escapeIdentifier($table);
        $sql = "INSERT INTO $tableName (" . implode(', ', $escapedFields) . ') VALUES ' . implode(', ', $lines);

        $this->db->exec($sql);
    }
}