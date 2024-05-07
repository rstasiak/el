<?php

namespace RStasiak\EL\Load\Oxla;

use RStasiak\EL\Contract\LoaderInterface;

class OxlaLoader implements LoaderInterface
{


    public function load(array $command, array $data)
    {

        $hasTable = $this->hasTable();
        $mode = $command['mode'];

        if ($hasTable && $mode == 'replace') {

            $this->deleteTable();
            $hasTable = false;

        }

        if ( ! $hasTable) {

            $this->createTable();

        }

        $this->loadToTable($data);


    }

}