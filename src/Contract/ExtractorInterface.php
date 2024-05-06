<?php

namespace RSETL\Contract;

use RSETL\Core\Collection;

interface ExtractorInterface
{
    public function extract(array $command):array;

}