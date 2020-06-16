<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;

final class NodeNotFound extends \RuntimeException implements GdbotsNcrException
{
    private ?NodeRef $nodeRef = null;

    public function __construct(string $message = '', ?\Throwable $previous = null)
    {
        parent::__construct($message, Code::NOT_FOUND, $previous);
    }

    public static function forNodeRef(NodeRef $nodeRef, ?\Throwable $previous = null): self
    {
        $e = new self("The node ({$nodeRef->toString()}) could not be found.", $previous);
        $e->nodeRef = $nodeRef;
        return $e;
    }

    public static function forIndex(string $index, string $value, ?\Throwable $previous = null): self
    {
        return new self("The node could not be found by ({$index}:{$value}).", $previous);
    }

    public function hasNodeRef(): bool
    {
        return null !== $this->nodeRef;
    }

    public function getNodeRef(): ?NodeRef
    {
        return $this->nodeRef;
    }
}
