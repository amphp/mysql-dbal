<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\Statement as SqlStatement;
use Doctrine\DBAL\Driver\Mysqli\Exception\UnknownType;
use Doctrine\DBAL\Driver\Mysqli\MysqliException;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Driver\StatementIterator;
use Doctrine\DBAL\FetchMode;
use Doctrine\DBAL\ParameterType;
use PDO;

class MysqlStatement implements Statement, \IteratorAggregate
{
    private const PARAM_TYPES = [
        ParameterType::NULL => true,
        ParameterType::INTEGER => true,
        ParameterType::STRING => true,
        ParameterType::ASCII => true,
        ParameterType::BINARY => true,
        ParameterType::LARGE_OBJECT => true,
        ParameterType::BOOLEAN => true,
    ];

    private SqlStatement $statement;
    private \Closure $resultListener;
    private MysqlResult $result;

    private array $values = [];
    private array $types = [];

    private array $columnNames;

    private int $defaultFetchMode = FetchMode::MIXED;

    public function __construct(SqlStatement $statement, callable $resultListener)
    {
        $this->statement = $statement;
        $this->resultListener = $resultListener instanceof \Closure
            ? $resultListener
            : \Closure::fromCallable($resultListener);
    }

    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        if (!isset(self::PARAM_TYPES[$type])) {
            throw UnknownType::new($type);
        }

        $key = \is_int($param) ? $param - 1 : $param;

        $this->values[$key] = $this->convertValue($value, $type);

        return true;
    }

    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        if (!isset(self::PARAM_TYPES[$type])) {
            throw UnknownType::new($type);
        }

        $key = \is_int($param) ? $param - 1 : $param;

        $this->values[$key] = &$variable;
        $this->types[$key] = $type;

        return true;
    }

    public function execute($params = null): Result
    {
        $values = $this->values;

        if ($params !== null) {
            foreach ($params as $param) {
                $values[] = $param;
            }
        }

        // Convert references to correct types
        foreach ($this->types as $param => $type) {
            $values[$param] = $this->convertValue($values[$param], $type);
        }

        try {
            $result = $this->statement->execute($values);
            ($this->resultListener)($result);

            $this->columnNames = $result->getFields() ?? [];

            return $this->result = new MysqlResult($result);
        } catch (\Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function closeCursor(): bool
    {
        $this->result->free();
        $this->statement->reset();

        return true;
    }

    public function columnCount(): int
    {
        return $this->result->columnCount();
    }

    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null): bool
    {
        $this->defaultFetchMode = $fetchMode;

        return true;
    }

    public function fetch($fetchMode = null, $cursorOrientation = PDO::FETCH_ORI_NEXT, $cursorOffset = 0)
    {
        if (!isset($this->result)) {
            return false;
        }

        return match ($fetchMode ?? $this->defaultFetchMode) {
            FetchMode::COLUMN => $this->fetchColumn(),
            FetchMode::NUMERIC => $this->result->fetchNumeric(),
            FetchMode::ASSOCIATIVE => $this->result->fetchAssociative(),
            FetchMode::MIXED => $this->fetchMixed(),
            FetchMode::STANDARD_OBJECT => (object) $this->result->fetchAssociative(),
            default => throw new MysqliException(sprintf("Unknown fetch type '%s'", $fetchMode)),
        };
    }

    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null)
    {
        $fetchMode = $fetchMode ?? $this->defaultFetchMode;

        $rows = [];

        if ($fetchMode === FetchMode::COLUMN) {
            while (($row = $this->fetchColumn()) !== false) {
                $rows[] = $row;
            }
        } else {
            while (($row = $this->fetch($fetchMode)) !== false) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    public function fetchColumn($columnIndex = 0)
    {
        $row = $this->fetch(FetchMode::NUMERIC);

        if ($row === false) {
            return false;
        }

        return $row[$columnIndex] ?? null;
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

    public function rowCount(): int
    {
        return $this->result->rowCount();
    }

    public function getIterator()
    {
        return new StatementIterator($this);
    }

    private function convertValue($value, int $type): null|bool|int|string
    {
        return match ($type) {
            ParameterType::NULL => null,
            ParameterType::INTEGER => (int) $value,
            ParameterType::ASCII, ParameterType::LARGE_OBJECT, ParameterType::BINARY, ParameterType::STRING => (string) $value,
            ParameterType::BOOLEAN => (bool) $value,
            default => throw UnknownType::new($type),
        };
    }

    private function fetchMixed(): array|bool
    {
        $row = $this->result->fetchNumeric();
        $assoc = array_combine($this->columnNames, $row);

        /** @noinspection AdditionOperationOnArraysInspection */
        return $assoc + $row;
    }
}