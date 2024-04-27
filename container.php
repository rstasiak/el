<?php


use DI\ContainerBuilder;

$containerBuilder = new ContainerBuilder();
$containerBuilder->addDefinitions(ROOT_DIR . '/config/definitions.php');
$containerBuilder->addDefinitions(ROOT_DIR . '/config/extractors.php');
$containerBuilder->addDefinitions(ROOT_DIR . '/config/loaders.php');
$containerBuilder->addDefinitions(ROOT_DIR . '/config/mappers.php');

return $containerBuilder->build();
