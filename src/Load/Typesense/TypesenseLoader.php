<?php

namespace RStasiak\EL\Load\Typesense;


use RStasiak\EL\Contract\LoaderInterface;
use Typesense\Client;

class TypesenseLoader implements LoaderInterface
{
    private Client $client;
    private int $batchSize;
    private bool $verbose;
    
    public function __construct(Client $client, int $batchSize = 100, bool $verbose = false)
    {
        $this->client = $client;
        $this->batchSize = $batchSize;
        $this->verbose = $verbose;
    }
    
    /**
     * Load data to Typesense
     * 
     * @param array $command [
     *   'collection' => 'name',
     *   'mode' => 'append|replace',
     *   'fields' => array of field definitions (optional),
     *   'filter' => filter for deletion (optional)
     * ]
     * @param array $data Data to import
     * @return array Import statistics
     */
    public function load(array $command, array $data)
    {
        if (empty($data)) {
            return ['imported' => 0, 'total' => 0, 'errors' => []];
        }
        
        $collection = $command['collection'] ?? throw new \InvalidArgumentException('Collection name required');
        $mode = $command['mode'] ?? 'append';
        $fields = $command['fields'] ?? [];
        $filter = $command['filter'] ?? null;
        
        // Check if collection exists
        $hasCollection = $this->hasCollection($collection);
        
        // Handle mode: replace
        if ($hasCollection && $mode === 'replace') {
            $this->deleteCollection($collection);
            $hasCollection = false;
            
            if ($this->verbose) {
                echo "Collection '$collection' deleted (mode: replace)\n";
            }
        }
        
        // Create collection if doesn't exist
        if (!$hasCollection) {
            $schema = $this->generateSchema($collection, $data, $fields);
            $this->createCollection($schema);
            
            if ($this->verbose) {
                echo "Collection '$collection' created with " . count($schema['fields']) . " fields\n";
            }
        } elseif ($mode === 'append' && $filter) {
            // In append mode with filter, delete matching documents
            $this->deleteDocuments($collection, $filter);
            
            if ($this->verbose) {
                echo "Deleted documents matching filter: $filter\n";
            }
        }
        
        // Import data in batches
        $chunks = array_chunk($data, $this->batchSize);
        $totalImported = 0;
        $errors = [];
        
        foreach ($chunks as $index => $chunk) {
            $result = $this->importChunk($collection, $chunk, $index);
            $totalImported += $result['imported'];
            $errors = array_merge($errors, $result['errors']);
        }
        
        return [
            'imported' => $totalImported,
            'total' => count($data),
            'errors' => array_unique($errors),
            'mode' => $mode,
            'collection' => $collection
        ];
    }
    
    private function hasCollection(string $collection): bool
    {
        try {
            $this->client->collections[$collection]->retrieve();
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }
    
    private function deleteCollection(string $collection): void
    {
        try {
            $this->client->collections[$collection]->delete();
        } catch (\Exception $e) {
            // Collection might not exist
        }
    }
    
    private function deleteDocuments(string $collection, string $filter): void
    {
        try {
            $this->client->collections[$collection]->documents->delete(['filter_by' => $filter]);
        } catch (\Exception $e) {
            // Documents might not exist
        }
    }
    
    private function generateSchema(string $collection, array $data, array $customFields): array
    {
        // Auto-generate schema from first row
        $firstRow = $data[0];
        $autoFields = [];
        
        foreach ($firstRow as $key => $value) {
            $type = $this->detectType($value);
            $autoFields[] = [
                'name' => $key,
                'type' => $type,
                'index' => in_array($key, ['id', 'name', 'ref_id']), // Auto-index common fields
                'optional' => is_null($value)
            ];
        }
        
        // Merge with custom fields if provided
        if (!empty($customFields)) {
            $fieldMap = [];
            foreach ($autoFields as $field) {
                $fieldMap[$field['name']] = $field;
            }
            
            foreach ($customFields as $customField) {
                if (isset($fieldMap[$customField['name']])) {
                    // Override auto field with custom definition
                    $fieldMap[$customField['name']] = array_merge($fieldMap[$customField['name']], $customField);
                } else {
                    // Add new field
                    $fieldMap[$customField['name']] = $customField;
                }
            }
            
            $autoFields = array_values($fieldMap);
        }
        
        return [
            'name' => $collection,
            'fields' => $autoFields
        ];
    }
    
    private function detectType($value): string
    {
        if (is_null($value)) {
            return 'string';
        }
        
        if (is_bool($value)) {
            return 'bool';
        }
        
        if (is_int($value)) {
            return 'int32';
        }
        
        if (is_float($value)) {
            return 'float';
        }
        
        if (is_array($value)) {
            return 'string[]';
        }
        
        // Try to detect datetime
        if (is_string($value) && preg_match('/^\d{4}-\d{2}-\d{2}/', $value)) {
            return 'string'; // Typesense doesn't have datetime, use string
        }
        
        return 'string';
    }
    
    private function createCollection(array $schema): void
    {
        $this->client->collections->create($schema);
    }
    
    private function importChunk(string $collection, array $chunk, int $index): array
    {
        $payload = implode("\n", array_map('json_encode', $chunk));
        
        try {
            $response = $this->client->collections[$collection]->documents->import($payload);
            $imported = $this->countSuccessful($response);
            $errors = $this->extractErrors($response);
            
            if ($this->verbose) {
                echo "Chunk #" . ($index + 1) . ": imported $imported/" . count($chunk) . " documents\n";
                if (!empty($errors)) {
                    echo "  Errors: " . implode(", ", array_slice($errors, 0, 3)) . "\n";
                }
            }
            
            return ['imported' => $imported, 'errors' => $errors];
        } catch (\Exception $e) {
            if ($this->verbose) {
                echo "Chunk #" . ($index + 1) . " failed: " . $e->getMessage() . "\n";
            }
            return ['imported' => 0, 'errors' => [$e->getMessage()]];
        }
    }
    
    private function countSuccessful(string $response): int
    {
        $count = 0;
        $lines = explode("\n", trim($response));
        
        foreach ($lines as $line) {
            if (!empty($line)) {
                $result = json_decode($line, true);
                if (isset($result['success']) && $result['success']) {
                    $count++;
                }
            }
        }
        
        return $count;
    }
    
    private function extractErrors(string $response): array
    {
        $errors = [];
        $lines = explode("\n", trim($response));
        
        foreach ($lines as $line) {
            if (!empty($line)) {
                $result = json_decode($line, true);
                if (isset($result['error'])) {
                    $errors[] = $result['error'];
                }
            }
        }
        
        return $errors;
    }
}