<?php

namespace RSETL\Extract\GoogleSheets;

use RSETL\Contract\ExtractInterface;
use RSETL\Core\Collection;
use RSETL\Extract\GoogleSheets\Backstage\GoogleSheetsGrabber;
use Monolog\Logger;

class GoogleSheetsExtract implements ExtractInterface
{

    public function __construct(private Logger $logger, private GoogleSheetsGrabber $grabber)
    {
    }

    public function extract(array $command): Collection
    {
        $this->logger->info('SheetsGrabber has started');

        $sheetId = $command['sheet_id'];
        $tabs = $command['tabs'];

        $output = [];

        foreach ($tabs as $tab) {

            $data = $this->grabber->getSheet($sheetId, $tab);

            foreach ($data as $row) {

                $row['tab'] = $tab;
                $output[] = $row;
            }
        }

        $count = count($output);
        $this->logger->info('data fetched (' . $count . ' rows)');

        $c = new Collection();
        $c->setRows($output);

        return $c;

    }


}