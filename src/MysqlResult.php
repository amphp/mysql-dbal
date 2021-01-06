<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\Result as SqlResult;
use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result;

class MysqlResult implements Result
{
    private SqlResult $result;

    public function __construct(SqlResult $result)
    {
        $this->result = $result;
    }

    public function fetchNumeric(): array|false
    {
        $row = $this->fetchAssociative();
        if ($row === false) {
            return false;
        }

        return \array_values($row);
    }

    public function fetchAssociative(): array|false
    {
        /** @noinspection ProperNullCoalescingOperatorUsageInspection */
        return $this->result->continue() ?? false;
    }

    public function fetchOne()
    {
        return FetchUtils::fetchOne($this);
    }

    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    public function rowCount(): int
    {
        return $this->result->getRowCount();
    }

    public function columnCount(): int
    {
        return \count($this->result->getFields());
    }

    public function free(): void
    {
        $this->result->dispose();
    }
}