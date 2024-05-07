<?php

namespace RStasiak\EL\Extract\GoogleSheets;

use RStasiak\EL\Contract\ExtractorInterface;
use RStasiak\EL\Extract\GoogleSheets\Backstage\GoogleSheetsGrabber;

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

        return $output;




    }


}