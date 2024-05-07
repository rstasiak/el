<?php

namespace RStasiak\EL\Service;

class SchemaHelper
{
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