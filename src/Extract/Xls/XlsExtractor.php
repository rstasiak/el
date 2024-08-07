<?php

namespace RStasiak\EL\Extract\Xls;

use League\Csv\Writer;
use PhpOffice\PhpSpreadsheet\Reader\Xls;

class XlsExtractor
{

    private string $namespace;

    public function __construct(private Xls $reader)
    {
    }

//$m->run($params['name'], $params['params'] ?? [], $namespace, $params['output'], $params['mapper']?? null);

    public function run(string $dir, string $range, string $collection) {

        $this->grabDir($dir, $range, $collection);

    }

    public function setNamespace(string $namespace) {

        $this->namespace = $namespace;

    }

    public function getNamespace(): string
    {
        return $this->namespace;
    }


    public function loadDir(string $path, $range)
    {
        $files =  array_diff(scandir($path), array('..', '.'));

        $data = [];

        foreach($files as $file) {

            $data = array_merge($data, $this->load($path . '/'. $file, $range));

        }

        return $data;

    }

    public function extract(array $command)
    {

        $path = $command['path'] ?? '';
        $range = $command['range'] ?? '';

        $sheet = $this->reader->load($path)->getSheet(0);
        $highestRow = $sheet->getHighestRow();

        if (preg_match('/:[A-Z]+$/i', $range)) {
            // Jeśli po dwukropku jest tylko litera, dodaj highestRow
            $range .= $highestRow;
        }



        return $sheet->rangeToArray(

            $range,
            null,
            true,
            false,
            true

        );


    }


    public function grabDir(string $dir, string $range, string $collection, string $schema = null)
    {


        $rows = $this->loadDir($dir, $range);
        $keys = array_keys($rows[0]);
        $colsNumber = count($keys);


        $csv = Writer::createFromString();

        $header = [];

        $schemaPath = ROOT_DIR . '/config/schemas/' . $schema . '.php';


        if (is_file($schemaPath))
        {

            $schemaFields = require $schemaPath;

            foreach($schemaFields as $arr)
            {
                $header[] = $arr['name'];
            }

        } else {

            for($i=0;$i<$colsNumber;$i++)
            {
                $header[] = 'field_' . $i + 1;
            }


        }



        $csv->insertOne($header);



        $cols = [];


        foreach($rows as $row)
        {

            $csv->insertOne($row);
        }

        $destPath = ROOT_DIR  . '/data/' . $this->namespace . '/' . $collection . '.csv';
        file_put_contents($destPath, $csv->toString());

    }
}