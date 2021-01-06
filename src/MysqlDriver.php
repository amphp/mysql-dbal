<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\CancellableConnector;
use Amp\Mysql\ConnectionConfig;
use Amp\Socket\StaticConnector;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection;
use function Amp\Socket\connector;

final class MysqlDriver extends Driver\AbstractMySQLDriver
{
    public function connect(array $params): Connection
    {
        $config = new ConnectionConfig($params['host'] ?? 'localhost',
            $params['port'] ?? ConnectionConfig::DEFAULT_PORT,
            $params['user'] ?? '', $params['password'] ?? '', $params['dbname'] ?? null, null,
            $params['charset'] ?? ConnectionConfig::DEFAULT_CHARSET);

        $connector = connector();
        if (isset($params['unix_socket'])) {
            $connector = new StaticConnector('unix:' . $params['unix_socket'], $connector);
        }

        try {
            return new MysqlConnection((new CancellableConnector($connector))->connect($config));
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }
}