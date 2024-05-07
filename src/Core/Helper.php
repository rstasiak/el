<?php

namespace RStasiak\EL\Core;


use Exception;

class Helper
{


    public static function numberOnly(string $value): float
    {

        $filteredValue = preg_replace('/[^\d,-]/', '', $value);
        $filteredValue = str_replace(',', '.', $filteredValue);
        return (float) $filteredValue;
    }

    public static function monthToDay(int $year, int $month, string $type = 'fiscal'): string
    {
        if ($type == 'fiscal') {

            if ($month > 9) {
                $month = $month - 9;
            } else {

                $month = $month + 3;
                $year = $year - 1;
            }

        }

        $last = cal_days_in_month(CAL_GREGORIAN, $month, $year);
        $str = $year.'-'.$month.'-'.$last;
        $date = \DateTime::createFromFormat("Y-m-d", $str);
        return $date->format("Ymd");
    }


    public static function prepareSchema(array $initialSchema, array $keys): Schema
    {

        $schema = new Schema();

        $allowedTypes = ['string', 'integer', 'boolean', 'float', 'date'];


        foreach ($keys as $key) {
            $schema->setField($key, 'string');
        }

        foreach ($initialSchema as $field) {
            $name = $field['name'];
            $type = $field['type'];

            if ( ! in_array($type, $allowedTypes)) {
                throw new Exception('unknown schema type: '.$type);
            }

            $schema->setField($name, $type);
        }

        return $schema;


    }

    public static function grossToTotal(float $value): float
    {


        $em = 0.0976 * $value;
        $rent = 0.065 * $value;
        $wyp = 0.0093 * $value;
        $fp = 0.0245 * $value;
        $fgsp = 0.001 * $value;

        $l = $em + $rent + $wyp + $fp + $fgsp;

        return $value + $l;

    }

    public static function mergeSchema(array $initial, array $overwrite): array
    {


        $result = [];
        foreach ($initial as $item) {
            $result[$item['name']] = $item;
        }

        foreach ($overwrite as $item) {
            if (isset($result[$item['name']])) {
                $result[$item['name']] = array_merge($result[$item['name']], $item);
            } else {
                $result[$item['name']] = $item;
            }
        }

        return array_values($result);
    }

    public static function generateInitialSchema(array $keys): array
    {

        $data = [];

        foreach ($keys as $key) {

            $data[] = [
                'name' => $key,
                'type' => 'string',
            ];

        }

        return $data;
    }


}