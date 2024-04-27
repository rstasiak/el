<?php

namespace RSETL\Extract\GoogleSheets\Backstage;


use Google_Service_Sheets;

class GoogleSheetsGrabber
{

    public function __construct(private Google_Service_Sheets $service)
    {
    }

    public function getSheet(string $sheetId, string $tab):array {

        [$tabName, $range] = explode('!', $tab);


        $response = $this->service->spreadsheets_values->get($sheetId, $tabName . '!' . $range);
        $rows = $response->getValues();

        $columnsCount = $this->getColumnsDistance($range);

        $keys = [];

        for ($i = 1; $i <= $columnsCount; $i++) {

            $keys[] = 'c' . $i;
        }

        $output = [];

        $rowId = 0;

        foreach ($rows as $row) {


            $rowId++;

            $row = array_pad($row, $columnsCount, '');

            if (count($row) <> count($keys))
            {
                throw new \Exception('bad number of cols (row ' . $rowId . ' cols, ' . count($row) . ' instead of ' . count($keys) . ')' .json_encode($row));
            }

            $output[] = array_combine($keys, $row);
        }


        return $output;


    }

    private function columnLetterToNumber($columnLetter):int {
        $number = 0;
        $length = strlen($columnLetter);

        for ($i = 0; $i < $length; $i++) {
            $number = $number * 26 + (ord($columnLetter[$i]) - ord('A') + 1);
        }

        return $number;
    }

    private function getColumnsDistance($range): int
    {
        list($start, $end) = explode(':', $range);

        // Wyciągnij litery kolumn z zakresu
        preg_match('/[A-Z]+/', $start, $startMatches);
        preg_match('/[A-Z]+/', $end, $endMatches);

        // Przelicz litery kolumn na ich numeryczne wartości
        $startNumber = $this->columnLetterToNumber($startMatches[0]);
        $endNumber = $this->columnLetterToNumber($endMatches[0]);

        // Oblicz różnicę i zwróć wynik
        return $endNumber - $startNumber + 1;  // +1, ponieważ zakres jest włączny
    }

}