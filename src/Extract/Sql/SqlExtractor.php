<?php

namespace RStasiak\EL\Extract\Sql;

use PDO;
use RStasiak\EL\Contract\ExtractorInterface;

class SqlExtractor implements ExtractorInterface
{

    public function __construct(private PDO $db)
    {
    }

    public function extract(array $command):array
    {
        $sql = $command['sql'];
        $stmt = $this->db->prepare($sql);
        $stmt->execute($command['params'] ?? []);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);

    }
}