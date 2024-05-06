<?php

namespace RSETL\Extract\GoogleSheets;

use RSETL\Contract\ExtractorInterface;
use RSETL\Core\Collection2;
use RSETL\Extract\GoogleSheets\Backstage\GoogleSheetsGrabber;
use Monolog\Logger;

class GoogleSheetsExtractor implements ExtractorInterface
{

    public function __construct(private GoogleSheetsGrabber $grabber)
    {
    }

    public function extract(array $command): array
    {

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

        print_r($output);die;



    }


}