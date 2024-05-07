<?php

namespace RStasiak\EL\Contract;

interface LoaderInterface
{

    public function load(array $command, array $data);

}