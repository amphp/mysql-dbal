<?php

namespace Amp\Mysql\DBAL;

use Doctrine\DBAL\Driver\AbstractException;

final class MysqlException extends AbstractException
{
    public static function new(\Throwable $exception): self
    {
        return new self($exception->getMessage(), null, $exception->getCode());
    }
}