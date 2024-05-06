<?php

namespace RSETL\Contract;

use RSETL\Core\Collection2;

interface LoaderInterface
{

    public function load(array $command, array $data);

}