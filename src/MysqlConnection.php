<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\Connection as SqlConnection;
use Amp\Mysql\Result as SqlResult;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use function Amp\await;
use function Amp\Pipeline\discard;

class MysqlConnection implements Connection, ServerInfoAwareConnection
{
    private SqlConnection $connection;
    private \Closure $resultListener;
    private mixed $lastInsertId;

    public function __construct(SqlConnection $connection)
    {
        $this->connection = $connection;
        $this->resultListener = fn(SqlResult $result) => $this->lastInsertId = $result->getLastInsertId();
    }

    public function prepare($sql): Statement
    {
        try {
            return new MysqlStatement($this->connection->prepare($sql), $this->resultListener);
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function query(): Statement
    {
        try {
            $statement = $this->prepare(\func_get_arg(0));
            $statement->execute();

            return $statement;
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function quote($value, $type = ParameterType::STRING)
    {
        throw new \Error("Not implemented, use prepared statements");
    }

    public function exec($sql): int
    {
        try {
            $result = $this->connection->execute($sql);
            ($this->resultListener)($result);

            return $result->getRowCount();
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function lastInsertId($name = null)
    {
        return $this->lastInsertId;
    }

    public function beginTransaction(): bool
    {
        try {
            await(discard($this->connection->query("START TRANSACTION")));

            return true;
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function commit(): bool
    {
        try {
            await(discard($this->connection->query("COMMIT")));

            return true;
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function rollBack(): bool
    {
        try {
            await(discard($this->connection->query("ROLLBACK")));

            return true;
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function getServerVersion(): string
    {
        return $this->query("SELECT @@version")->fetchColumn(0);
    }

    public function errorCode(): ?string
    {
        return null;
    }

    public function errorInfo(): array
    {
        return [
            'Error info unavailable',
            $this->errorCode(),
        ];
    }

    public function requiresQueryForServerVersion(): bool
    {
        return false;
    }
}