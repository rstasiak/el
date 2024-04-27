<?php


use Monolog\Handler\ErrorLogHandler;
use Monolog\Logger;

return [


    Logger::class => function () {

        $name = 'ETL';
        $logger = new Logger($name);
        $logger->pushHandler(new ErrorLogHandler(0));


        return $logger;

    },








];
