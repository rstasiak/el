<?php

namespace RStasiak\EL\Contract;

interface ExtractorInterface
{
    public function extract(array $command):array;

}