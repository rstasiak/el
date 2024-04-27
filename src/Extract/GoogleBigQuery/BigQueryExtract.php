<?php

namespace RSETL\Extract\GoogleBigQuery;

use RSETL\Contract\ExtractInterface;
use RSETL\Core\Collection;
use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\ExponentialBackoff;
use Monolog\Logger;


class BigQueryExtract implements ExtractInterface
{


    public function __construct(private BigQueryClient $client, private Logger $logger)
    {
    }

    public function extract(array $command):Collection
    {
        $this->logger->info('BigQueryExtract has started');

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

        $collection = new Collection();

        foreach ($rows as $row) {

            $collection->addRow($row);

        }

        return $collection;







    }

}