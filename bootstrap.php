<?php

//ini_set('display_errors', 0);
use Dotenv\Dotenv;

ini_set('memory_limit', -1);

//set_error_handler(function($severity, $message, $file, $line) {
//    throw new \ErrorException($message, 0, $severity, $file, $line);
//});

//use Dotenv\Dotenv;

define('ROOT_DIR', realpath(__DIR__));

require_once ROOT_DIR . "/vendor/autoload.php";

$dotenv = Dotenv::createImmutable(ROOT_DIR . '/secrets');
$dotenv->load();
