<?php

namespace RSETL\Extract\Sql;

use RSETL\Core\Collection;
use PDO;

class SqlExtract
{

    public function __construct(private PDO $db)
    {
    }

    public function extract(array $command, array $fields = []): Collection
    {
        $sql = $command['sql'];
        $stmt = $this->db->prepare($sql);
        $stmt->execute($command['params'] ?? []);
        $res = $stmt->fetchAll(PDO::FETCH_ASSOC);


        $collection = new Collection();
        $collection->setFields($fields);
        foreach($res as $row)
        {
            $collection->addRow($row);
        }

        return $collection;
    }
}