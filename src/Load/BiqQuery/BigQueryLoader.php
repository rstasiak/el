<?php

namespace RStasiak\EL\Load\BiqQuery;

use RStasiak\EL\Core\Helper;
use League\Csv\Writer;
use RStasiak\EL\Contract\LoaderInterface;
use Google\Cloud\BigQuery\BigQueryClient;

class BigQueryLoader implements LoaderInterface
{

    public function __construct(private BigQueryClient $client)
    {
    }

    public function load(array $command, array $data): void
    {

        $datasetId = $command['dataset'];
        $tableId = $command['table'];
        $fields = $command['fields'];

        $hasTable = $this->hasTable($datasetId, $tableId);

        $mode = $command['mode'];


        if ($hasTable && $mode == 'replace') {

            $this->deleteTable($datasetId, $tableId);
            $hasTable = false;


        }

        if ( ! $hasTable) {

            $initial = $this->generateInitialFields(array_keys($data[0]));
            $schema = Helper::mergeSchema($initial, $fields);

            $this->createTable($datasetId, $tableId, $schema);
        }

        $this->loadToTable($datasetId, $tableId, $data);


    }

    private function generateInitialFields(array $keys): array
    {

        $data = [];

        foreach($keys as $key) {

            $data[] = [
                'name' => $key,
                'type' => 'string'
            ];

        }

        return $data;

    }




    private function hasTable(string $datasetId, string $tableId): bool
    {
        $dataset = $this->client->dataset($datasetId);

        if ( ! $dataset->exists()) {
            return false;
        }

        $table = $dataset->table($tableId);

        return $table->exists();

    }

    private function createTable(string $datasetId, string $tableId, array $fields): void
    {
        $dataset = $this->client->dataset($datasetId);

        if ( ! $dataset->exists()) {

            $this->client->createDataset($datasetId);

        }

        $schema['schema'] = [


            'fields' => $fields
        ];



//        print_r($fields);die;

        $dataset->createTable($tableId, $schema);

    }

    private function deleteTable(mixed $datasetId, mixed $tableId): void
    {
        $dataset = $this->client->dataset($datasetId);
        $table = $dataset->table($tableId);
        $table->delete();

    }

    private function loadToTable(mixed $datasetId, mixed $tableId, array $data): void
    {
        $dataset = $this->client->dataset($datasetId);
        $table = $dataset->table($tableId);

        $csv = Writer::createFromString();
        $csv->insertAll($data);

        $path = ROOT_DIR . '/' . uniqid() . '.csv';
        file_put_contents($path, $csv->toString());


        $loadConfig = $table->load(fopen($path, 'r'))->sourceFormat('CSV');
        $job = $this->client->runJob($loadConfig);

        $job->reload();

        if (!$job->isComplete()) {

            throw new \Exception('Job has not yet completed', 500);
        }

        unlink($path);





    }

}