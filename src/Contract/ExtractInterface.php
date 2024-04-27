<?php

namespace RSETL\Contract;

use RSETL\Core\Collection;

interface ExtractInterface
{
    public function extract(array $command):Collection;

}