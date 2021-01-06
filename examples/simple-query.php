<?php

use Amp\Mysql\DBAL\MysqlDriver;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\FetchMode;

require __DIR__ . '/../vendor/autoload.php';

$conn = DriverManager::getConnection([
    'driverClass' => MysqlDriver::class,
    'user' => 'homestead',
    'password' => 'secret',
    'dbname' => 'homestead',
]);

$conn->executeStatement('CREATE TABLE IF NOT EXISTS articles (id int, headline varchar(100))');

$conn->insert('articles', [
    'headline' => 'Foobar'
]);

$sql = "SELECT * FROM articles";
$stmt = $conn->executeQuery($sql); // Simple, but has several drawbacks

while (($row = $stmt->fetch(FetchMode::ASSOCIATIVE)) !== false) {
    echo $row['headline'] . PHP_EOL;
}