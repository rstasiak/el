<?php

namespace RSETL\Contract;

use RSETL\Core\Collection;

interface LoaderInterface
{

    public function load(array $command, Collection $collection);

}