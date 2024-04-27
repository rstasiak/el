<?php

namespace RSETL\Load\Csv;

use RSETL\Core\Collection;
use RSETL\Load\Contract\LoaderInterface;
use League\Csv\Writer;

class CsvLoader implements LoaderInterface
{

    public function load(array $command, Collection $collection)
    {

        $path = $command['path'];

        if ($mode == 'replace') {

            $csv = Writer::createFromString();
            $csv->insertOne($collection->getKeys());


        }

        if ($mode == 'append') {

            $csv = Writer::createFromPath($path);

        }

        $csv->insertAll($collection->getRows());
        file_put_contents($command['path'], $csv->toString());

    }
}