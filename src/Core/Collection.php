<?php

namespace RSETL\Core;

class Collection
{


    private array $rows = [];
    private array $keys = [];
    private array $fields = [];

    public function setFields(array $fields)
    {

        foreach ($fields as $field) {

            $key = $field['name'];
            $type = $field['type'];

            $this->addKey($key, $type);
        }

    }

    public function flatten(string $field):array {

        $data = [];

        foreach($this->getRows() as $row)
        {
            $data[] = $row[$field];
        }

        return $data;

    }

    public function addRow(array $row)
    {

        $keys = array_keys($row);

        foreach ($keys as $key) {


            $this->addKey($key, 'string');

        }


        $this->rows[] = $row;


    }

    public function addKey(string $name, string $type)
    {

        $this->keys[$name] = $name;
        $this->fields[$name] = $type;

    }

    public function getRows(): array
    {


        return $this->rows;


    }

    public function map(callable $mapper) {

        $rows = $this->rows;
        unset($this->rows);

        $this->keys = [];
        $this->fields = [];

        foreach($rows as $row)
        {
            $mapped = $mapper($row);
            $this->addRow($mapped);

        }

    }


    public function getFields(): array
    {

        return $this->fields;
    }

    public function getKeys(): array
    {
        return $this->keys;
    }

    public function setRows(array $data)
    {
        foreach ($data as $row) {

            $this->addRow($row);
        }
    }

}