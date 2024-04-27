<?php

namespace RSETL\Load\Service;

use RSETL\Core\Collection;

class BulkInsertService
{

    public function create(Collection $collection): string
    {

        return $this->createRows($collection->getFields(), $collection->getRows());


    }

    private function createRows(array $fields, array $data): string
    {

        $rows = [];

        foreach($data as $row) {

            $rows[] = $this->createRow($fields, $row);

        }

        return implode(', ', $rows);

    }

    private function createRow(array $fields, array $row): string
    {
        $values = [];

        foreach($row as $key => $value)
        {
            $values[] = $this->createValue($key, $fields[$key], $value);
        }

        return '(' . implode(', ', $values) . ')';

    }

    private function createValue(string $key, string $type, mixed $value) {


        $quoted = ['string'];

        if (!is_null($value)) {

            $value = str_replace("'", "`", $value);

        }

        if (in_array($type, $quoted)) {



            return "'" . $value . "'";

        } else {

            return $value;
        }
    }
}