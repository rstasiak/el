<?php


namespace RStasiak\EL\Extract\GoogleBigQuery;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\ExponentialBackoff;
use RStasiak\EL\Contract\ExtractorInterface;


class BigQueryExtractor implements ExtractorInterface
{


    public function __construct(private BigQueryClient $client)
    {
    }

    public function extract(array $command):array
    {

        $sql = $command['sql'];

        $queryJobConfig = $this->client->query($sql);
        $queryResults = $this->client->runQuery($queryJobConfig);

// Ustawienie polityki ponawiania
        $backoff = new ExponentialBackoff(10);
        $backoff->execute(function () use ($queryResults) {
            $queryResults->reload();
            if (!$queryResults->isComplete()) {
                throw new Exception('Job has not yet completed', 500);
            }
        });

        $data = [];

        $rows = $queryResults->rows();

        foreach ($rows as $row) {

            $data[] = $row;

        }

        return $data;







    }

}