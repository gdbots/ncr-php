<?php

namespace Gdbots\Ncr\Exception;

use Gdbots\Pbj\Exception\HasEndUserMessage;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;

class NodeNotFound extends \RuntimeException implements GdbotsNcrException, HasEndUserMessage
{
    /** @var NodeRef */
    protected $nodeRef;

    /**
     * @param string          $message
     * @param \Exception|null $previous
     */
    public function __construct(string $message = '', ?\Exception $previous = null)
    {
        parent::__construct($message, Code::NOT_FOUND, $previous);
    }

    /**
     * @param NodeRef         $nodeRef
     * @param \Exception|null $previous
     *
     * @return NodeNotFound
     */
    public static function forNodeRef(NodeRef $nodeRef, ?\Exception $previous = null): self
    {
        $e = new self("The node ({$nodeRef->toString()}) could not be found.", $previous);
        $e->nodeRef = $nodeRef;

        return $e;
    }

    /**
     * @param string          $index
     * @param string          $value
     * @param \Exception|null $previous
     *
     * @return NodeNotFound
     */
    public static function forIndex(string $index, string $value, ?\Exception $previous = null): self
    {
        return new self("The node could not be found by ({$index}:{$value}).", $previous);
    }

    /**
     * {@inheritdoc}
     */
    public function getEndUserMessage()
    {
        return $this->getMessage();
    }

    /**
     * {@inheritdoc}
     */
    public function getEndUserHelpLink()
    {
        return null;
    }

    /**
     * @return bool
     */
    public function hasNodeRef(): bool
    {
        return null !== $this->nodeRef;
    }

    /**
     * @return NodeRef
     */
    public function getNodeRef(): NodeRef
    {
        return $this->nodeRef;
    }
}
